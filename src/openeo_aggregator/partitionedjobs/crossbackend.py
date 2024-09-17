from __future__ import annotations

import abc
import collections
import copy
import dataclasses
import datetime
import fractions
import functools
import itertools
import logging
import time
import types
from contextlib import nullcontext
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Union,
)

import openeo
from openeo import BatchJob
from openeo_driver.jobregistry import JOB_STATUS

from openeo_aggregator.constants import JOB_OPTION_FORCE_BACKEND
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.splitting import AbstractJobSplitter
from openeo_aggregator.utils import (
    _UNSET,
    FlatPG,
    PGWithMetadata,
    SkipIntermittentFailures,
)

_log = logging.getLogger(__name__)

_LOAD_RESULT_PLACEHOLDER = "_placeholder:"

# Some type annotation aliases to make things more self-documenting
CollectionId = str
SubGraphId = str
NodeId = str
BackendId = str


class GraphSplitException(Exception):
    pass


class GetReplacementCallable(Protocol):
    """
    Type annotation for callback functions that produce a node replacement
    for a node that is split off from the main process graph

    Also see `_default_get_replacement`
    """

    def __call__(self, node_id: str, node: dict, subgraph_id: SubGraphId) -> dict:
        """
        :param node_id: original id of the node in the process graph (e.g. `loadcollection2`)
        :param node: original node in the process graph (e.g. `{"process_id": "load_collection", "arguments": {...}}`)
        :param subgraph_id: id of the corresponding dependency subgraph
            (to be handled as opaque id, but possibly something like `backend1:loadcollection2`)

        :return: new process graph nodes. Should contain at least a node keyed under `node_id`
        """
        ...


def _default_get_replacement(node_id: str, node: dict, subgraph_id: SubGraphId) -> dict:
    """
    Default `get_replacement` function to replace a node that has been split off.
    """
    return {
        node_id: {
            # TODO: use `load_stac` iso `load_result`
            "process_id": "load_result",
            "arguments": {"id": f"{_LOAD_RESULT_PLACEHOLDER}{subgraph_id}"},
        }
    }


class _SubGraphData(NamedTuple):
    split_node: NodeId
    node_ids: Set[NodeId]
    backend_id: BackendId


class _PGSplitResult(NamedTuple):
    primary_node_ids: Set[NodeId]
    primary_backend_id: BackendId
    secondary_graphs: List[_SubGraphData]


class ProcessGraphSplitterInterface(metaclass=abc.ABCMeta):
    """
    Interface for process graph splitters:
    given a process graph (flat graph representation),
    produce a main graph and secondary graphs (as subsets of node ids)
    and the backends they are supposed to run on.
    """

    @abc.abstractmethod
    def split(self, process_graph: FlatPG) -> _PGSplitResult:
        """
        Split given process graph (flat graph representation) into sub graphs

        Returns primary graph data (node ids and backend id)
        and secondary graphs data (list of tuples: split node id, subgraph node ids,backend id)
        """
        ...


class LoadCollectionGraphSplitter(ProcessGraphSplitterInterface):
    """
    Simple process graph splitter that just splits off load_collection nodes.
    """

    def __init__(self, backend_for_collection: Callable[[CollectionId], BackendId], always_split: bool = False):
        # TODO: also support not not having a backend_for_collection map?
        self._backend_for_collection = backend_for_collection
        self._always_split = always_split

    def split(self, process_graph: FlatPG) -> _PGSplitResult:
        # Extract necessary back-ends from `load_collection` usage
        backend_per_collection: Dict[str, str] = {
            cid: self._backend_for_collection(cid)
            for cid in (
                node["arguments"]["id"] for node in process_graph.values() if node["process_id"] == "load_collection"
            )
        }
        backend_usage = collections.Counter(backend_per_collection.values())
        _log.info(f"Extracted backend usage from `load_collection` nodes: {backend_usage=} {backend_per_collection=}")

        # TODO: more options to determine primary backend?
        primary_backend = backend_usage.most_common(1)[0][0] if backend_usage else None
        secondary_backends = {b for b in backend_usage if b != primary_backend}
        _log.info(f"Backend split: {primary_backend=} {secondary_backends=}")

        primary_has_load_collection = False
        primary_graph_node_ids = set()
        secondary_graphs: List[_SubGraphData] = []
        for node_id, node in process_graph.items():
            if node["process_id"] == "load_collection":
                bid = backend_per_collection[node["arguments"]["id"]]
                if bid == primary_backend and (not self._always_split or not primary_has_load_collection):
                    primary_graph_node_ids.add(node_id)
                    primary_has_load_collection = True
                else:
                    secondary_graphs.append(_SubGraphData(split_node=node_id, node_ids={node_id}, backend_id=bid))
            else:
                primary_graph_node_ids.add(node_id)

        return _PGSplitResult(
            primary_node_ids=primary_graph_node_ids,
            primary_backend_id=primary_backend,
            secondary_graphs=secondary_graphs,
        )


class CrossBackendJobSplitter(AbstractJobSplitter):
    """
    Split a process graph, to be executed across multiple back-ends,
    based on availability of collections.

    .. warning::
        this is experimental functionality

    """

    def __init__(self, graph_splitter: ProcessGraphSplitterInterface):
        """
        :param backend_for_collection: callable that determines backend id for given collection id
        :param always_split: split all load_collections, also when on same backend
        """
        self._graph_splitter = graph_splitter

    def split_streaming(
        self,
        process_graph: FlatPG,
        get_replacement: GetReplacementCallable = _default_get_replacement,
        main_subgraph_id: SubGraphId = "main",
    ) -> Iterator[Tuple[SubGraphId, SubJob, List[SubGraphId]]]:
        """
        Split given process graph in sub-process graphs and return these as an iterator
        in an order so that a subgraph comes after all subgraphs it depends on
        (e.g. main "primary" graph comes last).

        The iterator approach allows working with a dynamic `get_replacement` implementation
        that can be adaptive to previously produced subgraphs
        (e.g. creating openEO batch jobs on the fly and injecting the corresponding batch job ids appropriately).

        :return: Iterator of tuples containing:
            - subgraph id, it's recommended to handle it as opaque id (but usually format '{backend_id}:{node_id}')
            - SubJob
            - dependencies as list of subgraph ids
        """

        graph_split_result = self._graph_splitter.split(process_graph=process_graph)

        primary_pg = {k: process_graph[k] for k in graph_split_result.primary_node_ids}
        primary_dependencies = []

        for node_id, subgraph_node_ids, backend_id in graph_split_result.secondary_graphs:
            # New secondary pg
            sub_id = f"{backend_id}:{node_id}"
            sub_pg = {k: v for k, v in process_graph.items() if k in subgraph_node_ids}
            # Add new `save_result` node to the subgraphs
            sub_pg["_agg_crossbackend_save_result"] = {
                # TODO: other/better choices for save_result format (e.g. based on backend support, cube type)?
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": node_id},
                    # TODO: particular format options?
                    # "format": "NetCDF",
                    "format": "GTiff",
                },
                "result": True,
            }
            yield (sub_id, SubJob(process_graph=sub_pg, backend_id=backend_id), [])

            # Link secondary pg into primary pg
            primary_pg.update(get_replacement(node_id=node_id, node=process_graph[node_id], subgraph_id=sub_id))
            primary_dependencies.append(sub_id)

        yield (
            main_subgraph_id,
            SubJob(process_graph=primary_pg, backend_id=graph_split_result.primary_backend_id),
            primary_dependencies,
        )

    def split(self, process: PGWithMetadata, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        """Split given process graph into a `PartitionedJob`"""

        subjobs: Dict[SubGraphId, SubJob] = {}
        dependencies: Dict[SubGraphId, List[SubGraphId]] = {}
        for sub_id, subjob, sub_dependencies in self.split_streaming(process_graph=process["process_graph"]):
            subjobs[sub_id] = subjob
            if sub_dependencies:
                dependencies[sub_id] = sub_dependencies

        return PartitionedJob(
            process=process,
            metadata=metadata,
            job_options=job_options,
            subjobs=PartitionedJob.to_subjobs_dict(subjobs),
            dependencies=dependencies,
        )


def _resolve_dependencies(process_graph: FlatPG, batch_jobs: Dict[str, BatchJob]) -> FlatPG:
    """
    Replace placeholders in given process graph
    based on given subjob_id to batch_job_id mapping.

    .. warning::
        this is experimental functionality
    """
    result = dict()
    for node_id, node in process_graph.items():
        if node["process_id"] == "load_result" and node["arguments"]["id"].startswith(_LOAD_RESULT_PLACEHOLDER):
            dep_id = node["arguments"]["id"].partition(_LOAD_RESULT_PLACEHOLDER)[-1]
            batch_job = batch_jobs[dep_id]
            _log.info(f"resolve_dependencies: replace placeholder {dep_id!r} with concrete {batch_job.job_id!r}")
            try:
                # Try to get "canonical" result URL (signed URL)
                links = batch_job.get_results().get_metadata()["links"]
                [result_url] = [k["href"] for k in links if k.get("rel") == "canonical"]
            except Exception as e:
                result_url = batch_job.get_results_metadata_url(full=True)
                _log.warning(
                    f"Failed to get canonical result metadata URL for {batch_job.job_id!r}: {e}. "
                    f"Falling back on default result metadata URL {result_url!r}."
                )
            result[node_id] = {
                "process_id": "load_result",
                "arguments": {"id": result_url},
            }
        else:
            result[node_id] = copy.deepcopy(node)
    return result


class SUBJOB_STATES:
    # initial state
    WAITING = "waiting"
    # Ready to be started on back-end (all dependencies are ready)
    READY = "ready"
    # subjob was started on back-end and is running as far as we know
    RUNNING = "running"
    # subjob has errored/canceled dependencies
    CANCELED = "canceled"
    # subjob finished successfully on back-end
    FINISHED = "finished"
    # subjob failed on back-end
    ERROR = "error"

    FINAL_STATES = {CANCELED, FINISHED, ERROR}


def _loop():
    """Infinite loop, logging counter and elapsed time with each step."""
    start = datetime.datetime.now()
    for i in itertools.count(start=1):
        elapsed = datetime.datetime.now() - start
        _log.info(f"Scheduling loop: step {i}, elapsed: {elapsed}")
        yield i


def run_partitioned_job(pjob: PartitionedJob, connection: openeo.Connection, fail_fast: bool = True) -> dict:
    """
    Run partitioned job (probably with dependencies between subjobs)
    with an active polling loop for tracking and scheduling the subjobs

    .. warning::
        this is experimental functionality

    :param pjob:
    :param connection:
    :return: mapping of subjob id to some run info: latest subjob state, batch job id (if any), ...
    """
    # Active job tracking/scheduling loop
    subjobs: Dict[str, SubJob] = pjob.subjobs
    dependencies: Dict[str, Sequence[str]] = pjob.dependencies
    _log.info(f"subjob dependencies: {dependencies}")
    # Map subjob_id to a state from SUBJOB_STATES
    # TODO: wrap these state structs for easier keeping track of setting and getting state
    states: Dict[str, str] = {k: "waiting" for k in subjobs.keys()}
    _log.info(f"Initial states: {states}")
    # Map subjob_id to a batch job instances
    batch_jobs: Dict[str, BatchJob] = {}

    if not fail_fast:
        skip_intermittent_failures = SkipIntermittentFailures(limit=3)
    else:
        skip_intermittent_failures = nullcontext()

    for _ in _loop():
        need_sleep = True
        for subjob_id, subjob in subjobs.items():
            _log.info(f"Current state {subjob_id=!r}: {states[subjob_id]}")

            # Check upstream deps of waiting subjobs
            if states[subjob_id] == SUBJOB_STATES.WAITING:
                dep_states = set(states[dep] for dep in dependencies.get(subjob_id, []))
                _log.info(f"Dependency states for {subjob_id=!r}: {dep_states}")
                if SUBJOB_STATES.ERROR in dep_states or SUBJOB_STATES.CANCELED in dep_states:
                    _log.info(f"Dependency failure: canceling {subjob_id=!r}")
                    states[subjob_id] = SUBJOB_STATES.CANCELED
                elif all(s == SUBJOB_STATES.FINISHED for s in dep_states):
                    _log.info(f"No unfulfilled dependencies: ready to start {subjob_id=!r}")
                    states[subjob_id] = SUBJOB_STATES.READY

            # Handle job (start, poll status, ...)
            if states[subjob_id] == SUBJOB_STATES.READY:
                try:
                    process_graph = _resolve_dependencies(subjob.process_graph, batch_jobs=batch_jobs)

                    _log.info(f"Starting new batch job for subjob {subjob_id!r} on backend {subjob.backend_id!r}")
                    # Create
                    batch_job = connection.create_job(
                        process_graph=process_graph,
                        title=f"Cross-back-end partitioned job: subjob {subjob_id}",
                        additional={
                            JOB_OPTION_FORCE_BACKEND: subjob.backend_id,
                        },
                    )
                    batch_jobs[subjob_id] = batch_job
                    # Start
                    batch_job.start_job()
                    states[subjob_id] = SUBJOB_STATES.RUNNING
                    _log.info(f"Started batch job {batch_job.job_id!r} for subjob {subjob_id!r}")
                except Exception as e:
                    if fail_fast:
                        raise
                    states[subjob_id] = SUBJOB_STATES.ERROR
                    _log.warning(
                        f"Failed to start batch job for subjob {subjob_id!r}: {e}",
                        exc_info=True,
                    )
            elif states[subjob_id] == SUBJOB_STATES.RUNNING:
                with skip_intermittent_failures:
                    # Check batch jobs status on backend
                    batch_job = batch_jobs[subjob_id]
                    batch_job_status = batch_job.status()
                    _log.info(
                        f"Upstream status for subjob {subjob_id!r} (batch job {batch_job.job_id!r}): {batch_job_status}"
                    )
                    if batch_job_status == JOB_STATUS.FINISHED:
                        states[subjob_id] = SUBJOB_STATES.FINISHED
                        need_sleep = False
                    elif batch_job_status in {
                        JOB_STATUS.ERROR,
                        JOB_STATUS.CANCELED,
                    }:
                        # TODO: fail fast here instead of keeping the whole partitioned job running?
                        states[subjob_id] = SUBJOB_STATES.ERROR
                        need_sleep = False
                    elif batch_job_status in {
                        JOB_STATUS.QUEUED,
                        JOB_STATUS.RUNNING,
                    }:
                        need_sleep = True
                    else:
                        raise ValueError(f"Unexpected {batch_job_status=}")

        state_stats = collections.Counter(states.values())
        _log.info(f"Current state overview: {states=} {state_stats=} {batch_jobs=}")

        if set(state_stats.keys()) == {SUBJOB_STATES.FINISHED}:
            _log.info("Breaking out of loop: all jobs finished successfully.")
            break
        elif set(state_stats.keys()).issubset(SUBJOB_STATES.FINAL_STATES):
            # TODO fail with exception instead of just returning a status report
            _log.warning("Breaking out of loop: some jobs failed.")
            break

        if need_sleep:
            _log.info("Going to sleep")
            time.sleep(30)
        else:
            _log.info("No time to sleep")

    return {
        sid: {
            "state": states[sid],
            "batch_job": batch_jobs.get(sid),
        }
        for sid in subjobs.keys()
    }


@dataclasses.dataclass(frozen=True)
class _FrozenNode:
    """
    Node in a _FrozenGraph, with pointers to other nodes it depends on (needs data/input from)
    and nodes to which it is input to.

    This is as immutable as possible (as far as Python allows) to
    be used and reused in iterative/recursive graph handling algorithms,
    without having to worry about accidentally changing state.
    """

    # TODO: instead of frozen dataclass: have __init__ with some type casting/validation. Or use attrs?
    # TODO: better name for this class?

    # Node ids of other nodes this node depends on (aka parents)
    depends_on: frozenset[NodeId]
    # Node ids of other nodes that depend on this node (aka children)
    flows_to: frozenset[NodeId]

    # Backend ids this node is marked to be supported on
    # value None means it is unknown/unconstrained for this node
    # TODO: Move this to _FrozenGraph as responsibility?
    backend_candidates: Union[frozenset[BackendId], None]

    def __repr__(self):
        return "".join(
            [
                f"Node ",
                f"@({','.join(self.backend_candidates) if self.backend_candidates else None})",
            ]
            + [f"<{d}" for d in self.depends_on]
            + [f">{f}" for f in self.flows_to]
        )


class _FrozenGraph:
    """
    Graph of _FrozenNode objects.
    """

    # TODO: find better class name: e.g. SplitGraphView, GraphSplitUtility, GraphSplitter, ...?
    # TODO: add more logging of what is happening under the hood

    def __init__(self, graph: dict[NodeId, _FrozenNode]):
        # Work with a read-only proxy to prevent accidental changes
        # TODO: check consistency of references?
        self._graph: Mapping[NodeId, _FrozenNode] = types.MappingProxyType(graph)

    def __repr__(self):
        return f"<{type(self).__name__}({self._graph})>"

    @classmethod
    def from_flat_graph(cls, flat_graph: FlatPG, backend_candidates_map: Dict[NodeId, Iterable[BackendId]]):
        """
        Build _FrozenGraph from a flat process graph representation
        """
        # Extract dependency links between nodes
        depends_on = collections.defaultdict(list)
        flows_to = collections.defaultdict(list)
        for node_id, node in flat_graph.items():
            for arg_value in node.get("arguments", {}).values():
                if isinstance(arg_value, dict) and list(arg_value.keys()) == ["from_node"]:
                    from_node = arg_value["from_node"]
                    depends_on[node_id].append(from_node)
                    flows_to[from_node].append(node_id)
        graph = {
            node_id: _FrozenNode(
                depends_on=frozenset(depends_on.get(node_id, [])),
                flows_to=frozenset(flows_to.get(node_id, [])),
                backend_candidates=(
                    # TODO move this logic to _FrozenNode.__init__
                    frozenset(backend_candidates_map.get(node_id))
                    if node_id in backend_candidates_map
                    else None
                ),
            )
            for node_id, node in flat_graph.items()
        }
        return cls(graph=graph)

    @classmethod
    def from_edges(
        cls,
        edges: Iterable[Tuple[NodeId, NodeId]],
        backend_candidates_map: Optional[Dict[NodeId, Iterable[BackendId]]] = None,
    ):
        """
        Simple factory to build graph from parent-child tuples for testing purposes
        """
        depends_on = collections.defaultdict(list)
        flows_to = collections.defaultdict(list)
        for parent, child in edges:
            depends_on[child].append(parent)
            flows_to[parent].append(child)

        graph = {
            node_id: _FrozenNode(
                # Note that we just use node id as process id. Do we have better options here?
                depends_on=frozenset(depends_on.get(node_id, [])),
                flows_to=frozenset(flows_to.get(node_id, [])),
                backend_candidates=(
                    frozenset(backend_candidates_map.get(node_id))
                    if backend_candidates_map and node_id in backend_candidates_map
                    else None
                ),
            )
            for node_id in set(depends_on.keys()).union(flows_to.keys())
        }
        return cls(graph=graph)

    def node(self, node_id: NodeId) -> _FrozenNode:
        if node_id not in self._graph:
            raise GraphSplitException(f"Invalid node id {node_id}.")
        return self._graph[node_id]

    def iter_nodes(self) -> Iterator[Tuple[NodeId, _FrozenNode]]:
        """Iterate through node_id-node pairs"""
        yield from self._graph.items()

    def _walk(
        self, seeds: Iterable[NodeId], next_nodes: Callable[[NodeId], Iterable[NodeId]], include_seeds: bool = True
    ) -> Iterator[NodeId]:
        """
        Walk the graph nodes starting from given seed nodes, taking steps as defined by `next_nodes` function.
        Optionally include seeds or not, and walk breadth first.
        """
        if include_seeds:
            visited = set()
            to_visit = list(seeds)
        else:
            visited = set(seeds)
            to_visit = [n for s in seeds for n in next_nodes(s)]

        while to_visit:
            node_id = to_visit.pop(0)
            if node_id in visited:
                continue
            yield node_id
            visited.add(node_id)
            to_visit.extend(set(next_nodes(node_id)).difference(visited))

    def walk_upstream_nodes(self, seeds: Iterable[NodeId], include_seeds: bool = True) -> Iterator[NodeId]:
        """
        Walk upstream nodes (along `depends_on` link) starting from given seed nodes.
        Optionally include seeds or not, and walk breadth first.
        """
        return self._walk(seeds=seeds, next_nodes=lambda n: self.node(n).depends_on, include_seeds=include_seeds)

    def walk_downstream_nodes(self, seeds: Iterable[NodeId], include_seeds: bool = True) -> Iterator[NodeId]:
        """
        Walk downstream nodes (along `flows_to` link) starting from given seed nodes.
        Optionally include seeds or not, and walk breadth first.
        """
        return self._walk(seeds=seeds, next_nodes=lambda n: self.node(n).flows_to, include_seeds=include_seeds)

    def get_backend_candidates_for_node(self, node_id: NodeId) -> Union[frozenset[BackendId], None]:
        """Determine backend candidates for given node id"""
        # TODO: cache intermediate sets? (Only when caching is safe: e.g. wrapped graph is immutable/not manipulated)
        if self.node(node_id).backend_candidates is not None:
            # Node has explicit backend candidates listed
            return self.node(node_id).backend_candidates
        elif self.node(node_id).depends_on:
            # Backend support is unset: determine it (as intersection) from upstream nodes
            return self.get_backend_candidates_for_node_set(self.node(node_id).depends_on)
        else:
            return None

    def get_backend_candidates_for_node_set(self, node_ids: Iterable[NodeId]) -> Union[frozenset[BackendId], None]:
        """
        Determine backend candidates for a set of nodes
        """
        candidates = set(self.get_backend_candidates_for_node(n) for n in node_ids)
        if candidates == {None}:
            return None
        candidates.discard(None)
        return functools.reduce(lambda a, b: a.intersection(b), candidates)

    def find_forsaken_nodes(self) -> Set[NodeId]:
        """
        Find nodes that have no backend candidates to process them
        """
        return set(
            node_id for (node_id, _) in self.iter_nodes() if self.get_backend_candidates_for_node(node_id) == set()
        )

    def find_articulation_points(self) -> Set[NodeId]:
        """
        Find articulation points (cut vertices) in the directed graph:
        nodes that when removed would split the graph into multiple sub-graphs.

        Note that, unlike in traditional graph theory, the search also includes leaf nodes
        (e.g. nodes with no parents), as in this context of openEO graph splitting,
        when we "cut" a node, we replace it with two disconnected new nodes
        (one connecting to the original parents and one connecting to the original children).
        """
        # Approach: label the start nodes (e.g. load_collection) with their id and weight 1.
        # Propagate these labels along the depends-on links, but split/sum the weight according
        # to the number of children/parents.
        # At the end: the articulation points are the nodes where all flows have weight 1.

        # Mapping: node_id -> start_node_id -> flow_weight
        flow_weights: Dict[NodeId, Dict[NodeId, fractions.Fraction]] = {}

        # Initialize at the pure input nodes (nodes with no upstream dependencies)
        for node_id, node in self.iter_nodes():
            if not node.depends_on:
                flow_weights[node_id] = {node_id: fractions.Fraction(1, 1)}

        # Propagate flow weights using recursion + caching
        def get_flow_weights(node_id: NodeId) -> Dict[NodeId, fractions.Fraction]:
            nonlocal flow_weights
            if node_id not in flow_weights:
                flow_weights[node_id] = {}
                # Calculate from upstream nodes
                for upstream in self.node(node_id).depends_on:
                    for start_node_id, weight in get_flow_weights(upstream).items():
                        flow_weights[node_id].setdefault(start_node_id, fractions.Fraction(0, 1))
                        flow_weights[node_id][start_node_id] += weight / len(self.node(upstream).flows_to)
            return flow_weights[node_id]

        for node_id, node in self.iter_nodes():
            get_flow_weights(node_id)

        # Select articulation points: nodes where all flows have weight 1
        return set(node_id for node_id, flows in flow_weights.items() if all(w == 1 for w in flows.values()))

    def split_at(self, split_node_id: NodeId) -> Tuple[_FrozenGraph, _FrozenGraph]:
        """
        Split graph at given node id (must be articulation point),
        creating two new graphs, containing original nodes and adaptation of the split node.

        :return: two _FrozenGraph objects: the upstream subgraph and the downstream subgraph
        """
        split_node = self.node(split_node_id)

        # Walk the graph, upstream from the split node
        def next_nodes(node_id: NodeId) -> Iterable[NodeId]:
            node = self.node(node_id)
            if node_id == split_node_id:
                return node.depends_on
            else:
                return node.depends_on.union(node.flows_to)

        up_node_ids = set(self._walk(seeds=[split_node_id], next_nodes=next_nodes))

        if split_node.flows_to.intersection(up_node_ids):
            raise GraphSplitException(f"Graph can not be split at {split_node_id}: not an articulation point.")

        up_graph = {n: self.node(n) for n in up_node_ids}
        up_graph[split_node_id] = _FrozenNode(
            depends_on=split_node.depends_on,
            flows_to=frozenset(),
            backend_candidates=split_node.backend_candidates,
        )
        up = _FrozenGraph(graph=up_graph)

        down_graph = {n: node for n, node in self.iter_nodes() if n not in up_node_ids}
        down_graph[split_node_id] = _FrozenNode(
            depends_on=frozenset(),
            flows_to=split_node.flows_to,
            backend_candidates=None,
        )
        down = _FrozenGraph(graph=down_graph)

        return up, down

    def produce_split_locations(self, limit: int = 2) -> Iterator[List[NodeId]]:
        """
        Produce disjoint subgraphs that can be processed independently.

        :return: iterator of node listings.
            Each node listing encodes a graph split (nodes ids where to split).
            A node listing is ordered with the following in mind:
            - the first node id does a first split in a downstream and upstream part.
              The upstream part can be handled by a single backend.
              The downstream part is not necessarily covered by a single backend,
              in which case one or more additional splits will be necessary.
            - the second node id does a second split of the downstream part of
              the previous split.
            - etc
        """
        # Find nodes that have empty set of backend_candidates
        forsaken_nodes = self.find_forsaken_nodes()

        if forsaken_nodes:
            # Sort forsaken nodes (based on forsaken parent count), to start higher up the graph
            # TODO: avoid need for this sort, and just use a better scoring metric higher up?
            forsaken_nodes = sorted(
                forsaken_nodes, reverse=True, key=lambda n: sum(p in forsaken_nodes for p in self.node(n).depends_on)
            )
            # Collect nodes where we could split the graph in disjoint subgraphs
            articulation_points: Set[NodeId] = set(self.find_articulation_points())

            # TODO: allow/deny lists of what openEO processes can be split on? E.g. only split raster cube paths

            # Walk upstream from forsaken nodes to find articulation points, where we can cut
            split_options = [
                n
                for n in self.walk_upstream_nodes(seeds=forsaken_nodes, include_seeds=False)
                if n in articulation_points
            ]
            if not split_options:
                raise GraphSplitException("No split options found.")
            # TODO: how to handle limit? will it scale feasibly to iterate over all possibilities at this point?
            # TODO: smarter picking of split node (e.g. one with most upstream nodes)
            for split_node_id in split_options[:limit]:
                # Split graph at this articulation point
                up, down = self.split_at(split_node_id)
                if down.find_forsaken_nodes():
                    down_splits = list(down.produce_split_locations(limit=limit - 1))
                else:
                    down_splits = [[]]
                if up.find_forsaken_nodes():
                    # TODO: will this actually happen? the upstream sub-graph should be single-backend by design?
                    up_splits = list(up.produce_split_locations(limit=limit - 1))
                else:
                    up_splits = [[]]

                for down_split, up_split in itertools.product(down_splits, up_splits):
                    yield [split_node_id] + down_split + up_split

        else:
            # All nodes can be handled as is, no need to split
            yield []


class DeepGraphSplitter(ProcessGraphSplitterInterface):
    """
    More advanced graph splitting (compared to just splitting off `load_collection` nodes)
    """

    # TODO: unify:
    #       - backend_for_collection: Callable[[CollectionId], BackendId]
    #       - backend_candidates_map: Dict[NodeId, Iterable[BackendId]]
    #       Note that the nodeid-backendid mapping smells like bad decoupling
    #       as the process graph is given to split methods, while mapping to __init__
    # TODO: validation for Iterable[BackendId]  (avoid passing a single string instead of iterable of strings)
    def __init__(self, backend_candidates_map: Dict[NodeId, Iterable[BackendId]]):
        self._backend_candidates_map = backend_candidates_map

    def split(self, process_graph: FlatPG) -> _PGSplitResult:
        graph = _FrozenGraph.from_flat_graph(
            flat_graph=process_graph, backend_candidates_map=self._backend_candidates_map
        )

        # TODO: make picking "optimal" split location set a bit more deterministic (e.g. sort first)
        (split_nodes,) = graph.produce_split_locations(limit=1)

        secondary_graphs: List[_SubGraphData] = []
        graph_to_split = graph
        for split_node_id in split_nodes:
            up, down = graph_to_split.split_at(split_node_id=split_node_id)
            # Use upstream graph as secondary graph
            node_ids = set(nid for nid, _ in up.iter_nodes())
            backend_candidates = up.get_backend_candidates_for_node_set(node_ids)
            # TODO: better backend selection?
            # TODO handle case where backend_candidates is None?
            backend_id = sorted(backend_candidates)[0]
            secondary_graphs.append(
                _SubGraphData(
                    split_node=split_node_id,
                    node_ids=node_ids,
                    backend_id=backend_id,
                )
            )

            # Prepare for next split (if any)
            graph_to_split = down

        # Remaining graph is primary graph
        primary_graph = graph_to_split
        primary_node_ids = set(n for n, _ in primary_graph.iter_nodes())
        backend_candidates = primary_graph.get_backend_candidates_for_node_set(primary_node_ids)
        primary_backend_id = sorted(backend_candidates)[0]

        return _PGSplitResult(
            primary_node_ids=primary_node_ids,
            primary_backend_id=primary_backend_id,
            secondary_graphs=secondary_graphs,
        )
