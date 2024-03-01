import collections
import copy
import datetime
import itertools
import logging
import time
from contextlib import nullcontext
from typing import Callable, Dict, Iterator, List, Optional, Protocol, Sequence, Tuple

import openeo
from openeo import BatchJob
from openeo_driver.jobregistry import JOB_STATUS

from openeo_aggregator.constants import JOB_OPTION_FORCE_BACKEND
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.splitting import AbstractJobSplitter
from openeo_aggregator.utils import FlatPG, PGWithMetadata, SkipIntermittentFailures

_log = logging.getLogger(__name__)

_LOAD_RESULT_PLACEHOLDER = "_placeholder:"

# Some type annotation aliases to make things more self-documenting
SubGraphId = str


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


class CrossBackendSplitter(AbstractJobSplitter):
    """
    Split a process graph, to be executed across multiple back-ends,
    based on availability of collections.

    .. warning::
        this is experimental functionality

    """

    def __init__(self, backend_for_collection: Callable[[str], str], always_split: bool = False):
        """
        :param backend_for_collection: callable that determines backend id for given collection id
        :param always_split: split all load_collections, also when on same backend
        """
        # TODO: just handle this `backend_for_collection` callback with a regular method?
        self.backend_for_collection = backend_for_collection
        self._always_split = always_split

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
        that adapting to on previously produced subgraphs
        (e.g. creating openEO batch jobs on the fly and injecting the corresponding batch job ids appropriately).

        :return: tuple containing:
            - subgraph id, recommended to handle it as opaque id (but usually format '{backend_id}:{node_id}')
            - SubJob
            - dependencies as list of subgraph ids
        """

        # Extract necessary back-ends from `load_collection` usage
        backend_per_collection: Dict[str, str] = {
            cid: self.backend_for_collection(cid)
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

        primary_id = main_subgraph_id
        primary_pg = {}
        primary_has_load_collection = False
        primary_dependencies = []

        for node_id, node in process_graph.items():
            if node["process_id"] == "load_collection":
                bid = backend_per_collection[node["arguments"]["id"]]
                if bid == primary_backend and (not self._always_split or not primary_has_load_collection):
                    # Add to primary pg
                    primary_pg[node_id] = node
                    primary_has_load_collection = True
                else:
                    # New secondary pg
                    sub_id = f"{bid}:{node_id}"
                    sub_pg = {
                        node_id: node,
                        "sr1": {
                            # TODO: other/better choices for save_result format (e.g. based on backend support)?
                            "process_id": "save_result",
                            "arguments": {
                                "data": {"from_node": node_id},
                                # TODO: particular format options?
                                # "format": "NetCDF",
                                "format": "GTiff",
                            },
                            "result": True,
                        },
                    }

                    yield (sub_id, SubJob(process_graph=sub_pg, backend_id=bid), [])

                    # Link secondary pg into primary pg
                    primary_pg.update(get_replacement(node_id=node_id, node=node, subgraph_id=sub_id))
                    primary_dependencies.append(sub_id)
            else:
                primary_pg[node_id] = node

        yield (primary_id, SubJob(process_graph=primary_pg, backend_id=primary_backend), primary_dependencies)

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
