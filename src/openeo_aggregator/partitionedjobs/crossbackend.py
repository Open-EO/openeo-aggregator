import collections
import copy
import functools
import itertools
import logging
import time
from typing import Callable, Dict, List, Sequence

import openeo
from openeo.util import ContextTimer
from openeo_driver.jobregistry import JOB_STATUS

from openeo_aggregator.constants import JOB_OPTION_FORCE_BACKEND
from openeo_aggregator.metadata import STAC_PROPERTY_FEDERATION_BACKENDS
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.splitting import AbstractJobSplitter
from openeo_aggregator.utils import FlatPG, PGWithMetadata

_log = logging.getLogger(__name__)

_LOAD_RESULT_PLACEHOLDER = "_placeholder:"


class CrossBackendSplitter(AbstractJobSplitter):
    """
    Split a process graph, to be executed across multiple back-ends,
    based on availability of collections
    """

    def __init__(
        self, backend_for_collection: Callable[[str], str], always_split: bool = False
    ):
        """

        :param backend_for_collection: callable that determines backend id for given collection id
        :param always_split: split all load_collections, also when on same backend
        """
        # TODO: just handle this `backend_for_collection` callback with a regular method?
        self.backend_for_collection = backend_for_collection
        self._always_split = always_split

    def split(
        self, process: PGWithMetadata, metadata: dict = None, job_options: dict = None
    ) -> PartitionedJob:
        process_graph = process["process_graph"]

        # Extract necessary back-ends from `load_collection` usage
        backend_usage = collections.Counter(
            self.backend_for_collection(node["arguments"]["id"])
            for node in process_graph.values()
            if node["process_id"] == "load_collection"
        )
        _log.info(
            f"Extracted backend usage from `load_collection` nodes: {backend_usage}"
        )

        primary_backend = backend_usage.most_common(1)[0][0] if backend_usage else None
        secondary_backends = {b for b in backend_usage if b != primary_backend}
        _log.info(f"Backend split: {primary_backend=} {secondary_backends=}")

        primary_id = "primary"
        primary_pg = SubJob(process_graph={}, backend_id=primary_backend)
        primary_has_load_collection = False

        subjobs: Dict[str, SubJob] = {primary_id: primary_pg}
        dependencies: Dict[str, List[str]] = {primary_id: []}

        for node_id, node in process_graph.items():
            if node["process_id"] == "load_collection":
                bid = self.backend_for_collection(node["arguments"]["id"])
                if bid == primary_backend and not (
                    self._always_split and primary_has_load_collection
                ):
                    primary_pg.process_graph[node_id] = node
                    primary_has_load_collection = True
                else:
                    # New secondary pg
                    pg = {
                        node_id: node,
                        "sr1": {
                            # TODO: other/better choices for save_result format (e.g. based on backend support)?
                            # TODO: particular format options?
                            "process_id": "save_result",
                            "arguments": {
                                "data": {"from_node": node_id},
                                # "format": "NetCDF",
                                "format": "GTiff",
                            },
                            "result": True,
                        },
                    }
                    dependency_id = f"{bid}:{node_id}"
                    subjobs[dependency_id] = SubJob(process_graph=pg, backend_id=bid)
                    dependencies[primary_id].append(dependency_id)
                    # Link to primary pg with load_result
                    primary_pg.process_graph[node_id] = {
                        # TODO: encapsulate this placeholder process/id better?
                        "process_id": "load_result",
                        "arguments": {
                            "id": f"{_LOAD_RESULT_PLACEHOLDER}{dependency_id}"
                        },
                    }
            else:
                primary_pg.process_graph[node_id] = node

        return PartitionedJob(
            process=process,
            metadata=metadata,
            job_options=job_options,
            subjobs=PartitionedJob.to_subjobs_dict(subjobs),
            dependencies=dependencies,
        )


def resolve_dependencies(
    process_graph: FlatPG, batch_job_ids: Dict[str, str]
) -> FlatPG:
    """Replace placeholders in given process graph based on given subjob_id to batch_job_id mapping."""
    result = dict()
    for node_id, node in process_graph.items():
        if node["process_id"] == "load_result" and node["arguments"]["id"].startswith(
            _LOAD_RESULT_PLACEHOLDER
        ):
            dep_id = node["arguments"]["id"].partition(_LOAD_RESULT_PLACEHOLDER)[-1]
            batch_job_id = batch_job_ids[dep_id]
            _log.info(f"resolve_dependencies: replace {dep_id} with {batch_job_id}")
            result[node_id] = {
                "process_id": "load_result",
                "arguments": {"id": batch_job_id},
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


class SkipIntermittentFailures:
    """
    Context manager for skipping intermittent failures.
    It swallows exceptions, but only up to a certain point:
    if there are too many successive failures,
    it will not block exceptions anymore.
    """

    # TODO: not only look at successive failures, but also fail rate?

    def __init__(self, limit: int = 5):
        self._limit = limit
        self._successive_failures = 0

    def __enter__(self):
        return

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._successive_failures += 1
            if self._successive_failures > self._limit:
                _log.error(
                    f"Failure tolerance exceeded ({self._successive_failures} > {self._limit}) with {exc_val!r}"
                )
                # Enough already!
                return False
            else:
                _log.warning(
                    f"Swallowing exception {exc_val!r} ({self._successive_failures} < {self._limit})"
                )
                return True
        else:
            # Reset counter of successive failures
            self._successive_failures = 0


def _poc():
    temporal_extent = ["2022-09-01", "2022-09-10"]
    spatial_extent = {"west": 3, "south": 51, "east": 3.1, "north": 51.1}
    process_graph = {
        "lc1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TERRASCOPE_S2_TOC_V2",
                "temporal_extent": temporal_extent,
                "spatial_extent": spatial_extent,
                "bands": ["B02", "B03"],
            },
        },
        "lc2": {
            # "process_id": "load_collection",
            # "arguments": {
            #     "id": "TERRASCOPE_S2_TOC_V2",
            #     "temporal_extent": temporal_extent,
            #     "spatial_extent": spatial_extent,
            #     "bands": ["tropno2"],
            # },
            "process_id": "load_collection",
            "arguments": {
                "id": "TERRASCOPE_S2_TOC_V2",
                "temporal_extent": temporal_extent,
                "spatial_extent": spatial_extent,
                "bands": ["B04"],
            },
        },
        "mc1": {
            "process_id": "merge_cubes",
            "arguments": {
                "cube1": {"from_node": "lc1"},
                "cube2": {"from_node": "lc2"},
            },
        },
        "sr1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
            "result": True,
        },
    }

    connection = openeo.connect("openeocloud-dev.vito.be").authenticate_oidc()

    @functools.lru_cache(maxsize=100)
    def backend_for_collection(collection_id) -> str:
        metadata = connection.describe_collection(collection_id)
        return metadata["summaries"][STAC_PROPERTY_FEDERATION_BACKENDS][0]

    splitter = CrossBackendSplitter(
        backend_for_collection=backend_for_collection, always_split=True
    )
    pjob = splitter.split({"process_graph": process_graph})
    print(pjob)

    # Active job tracking/scheduling loop
    subjobs: Dict[str, SubJob] = pjob.subjobs
    dependencies: Dict[str, Sequence[str]] = pjob.dependencies
    _log.info(f"subjob dependencies: {dependencies}")
    states = {k: "waiting" for k in subjobs.keys()}
    _log.info(f"Initial states: {states}")
    batch_job_ids: Dict[str, str] = {}

    skip_intermittent_failures = SkipIntermittentFailures(limit=3)

    with ContextTimer() as timer:
        for i in itertools.count():
            _log.info(f"Scheduling loop: step {i} (elapsed {timer.elapsed():.1f}s)")

            for subjob_id, subjob in subjobs.items():
                # Check upstream deps of waiting subjobs
                if states[subjob_id] == SUBJOB_STATES.WAITING:
                    dep_states = set(
                        states[dep] for dep in dependencies.get(subjob_id, [])
                    )
                    _log.info(f"Dependency states for {subjob_id=!r}: {dep_states}")
                    if (
                        SUBJOB_STATES.ERROR in dep_states
                        or SUBJOB_STATES.CANCELED in dep_states
                    ):
                        _log.info(f"Dependency failure: canceling {subjob_id=!r}")
                        states[subjob_id] = SUBJOB_STATES.CANCELED
                    elif all(s == SUBJOB_STATES.FINISHED for s in dep_states):
                        _log.info(
                            f"No unfulfilled dependencies: ready to start {subjob_id=!r}"
                        )
                        states[subjob_id] = SUBJOB_STATES.READY

                # Handle job (start, poll status, ...)
                if states[subjob_id] == SUBJOB_STATES.READY:
                    try:
                        process_graph = resolve_dependencies(
                            subjob.process_graph, batch_job_ids=batch_job_ids
                        )
                        batch_job = connection.create_job(
                            process_graph=process_graph,
                            title=f"Cross-back-end partitioned job: subjob {subjob_id}",
                            additional={
                                JOB_OPTION_FORCE_BACKEND: subjob.backend_id,
                            },
                        )
                        batch_job_ids[subjob_id] = batch_job.job_id
                        _log.info(
                            f"Created batch job {batch_job.job_id!r} for subjob {subjob_id!r}"
                        )
                        batch_job.start_job()
                        states[subjob_id] = SUBJOB_STATES.RUNNING
                        _log.info(
                            f"Started batch job {batch_job.job_id!r} for subjob {subjob_id!r}"
                        )
                    except Exception as e:
                        states[subjob_id] = SUBJOB_STATES.ERROR
                        _log.warning(
                            f"Failed to start batch job for subjob {subjob_id!r}: {e}",
                            exc_info=True,
                        )
                elif states[subjob_id] == SUBJOB_STATES.RUNNING:
                    with skip_intermittent_failures:
                        # Check batch jobs status on backend
                        batch_job_id = batch_job_ids[subjob_id]
                        batch_job_status = connection.job(job_id=batch_job_id).status()
                        _log.info(
                            f"Upstream status for subjob {subjob_id!r} (batch job {batch_job_id!r}): {batch_job_status}"
                        )
                        if batch_job_status == JOB_STATUS.FINISHED:
                            states[subjob_id] = SUBJOB_STATES.FINISHED
                        elif batch_job_status in {
                            JOB_STATUS.ERROR,
                            JOB_STATUS.CANCELED,
                        }:
                            states[subjob_id] = SUBJOB_STATES.ERROR
                        elif batch_job_status in {
                            JOB_STATUS.QUEUED,
                            JOB_STATUS.RUNNING,
                        }:
                            pass
                        else:
                            raise ValueError(f"Unexpected {batch_job_status=}")

            _log.info(f"Current state overview: {states=} {batch_job_ids=}")

            if all(
                s
                in {
                    SUBJOB_STATES.FINISHED,
                    SUBJOB_STATES.ERROR,
                    SUBJOB_STATES.CANCELED,
                }
                for s in states.values()
            ):
                _log.info("Breaking out of loop as all jobs are finished or failed")
                break

            # TODO: only sleep if there were no important status changes (don't sleep if a job finished for example)
            _log.info("Going to sleep")
            time.sleep(30)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _poc()
