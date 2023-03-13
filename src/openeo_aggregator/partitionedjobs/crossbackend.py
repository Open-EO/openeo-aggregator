import collections
import logging
from typing import Callable, Dict, List

from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.splitting import AbstractJobSplitter
from openeo_aggregator.utils import PGWithMetadata

_log = logging.getLogger(__name__)


class CrossBackendSplitter(AbstractJobSplitter):
    """
    Split a process graph, to be executed across multiple back-ends,
    based on availability of collections
    """

    def __init__(self, backend_for_collection: Callable[[str], str]):
        # TODO: just handle this `backend_for_collection` callback with a regular method?
        self.backend_for_collection = backend_for_collection

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

        subjobs: Dict[str, SubJob] = {primary_id: primary_pg}
        dependencies: Dict[str, List[str]] = {primary_id: []}

        for node_id, node in process_graph.items():
            if node["process_id"] == "load_collection":
                bid = self.backend_for_collection(node["arguments"]["id"])
                if bid == primary_backend:
                    primary_pg.process_graph[node_id] = node
                else:
                    # New secondary pg
                    pg = {
                        node_id: node,
                        "sr1": {
                            # TODO: other/better choices for save_result format (e.g. based on backend support)?
                            # TODO: particular format options?
                            "process_id": "save_result",
                            "arguments": {"format": "NetCDF"},
                        },
                    }
                    dependency_id = f"{bid}:{node_id}"
                    subjobs[dependency_id] = SubJob(process_graph=pg, backend_id=bid)
                    dependencies[primary_id].append(dependency_id)
                    # Link to primary pg with load_result
                    primary_pg.process_graph[node_id] = {
                        # TODO: encapsulate this placeholder process/id better?
                        "process_id": "load_result",
                        "arguments": {"id": f"placeholder:{dependency_id}"},
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
