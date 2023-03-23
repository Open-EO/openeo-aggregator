import functools
import logging

import openeo
from openeo.util import TimingLogger

from openeo_aggregator.metadata import STAC_PROPERTY_FEDERATION_BACKENDS
from openeo_aggregator.partitionedjobs import PartitionedJob
from openeo_aggregator.partitionedjobs.crossbackend import (
    CrossBackendSplitter,
    run_partitioned_job,
)

_log = logging.getLogger("crossbackend-poc")


def main():
    logging.basicConfig(level=logging.INFO)

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

    backend_url = "openeocloud-dev.vito.be"
    # backend_url = "openeo.cloud"

    with TimingLogger(title=f"Connecting to {backend_url}", logger=_log):
        connection = openeo.connect(url=backend_url).authenticate_oidc()

    @functools.lru_cache(maxsize=100)
    def backend_for_collection(collection_id) -> str:
        metadata = connection.describe_collection(collection_id)
        return metadata["summaries"][STAC_PROPERTY_FEDERATION_BACKENDS][0]

    splitter = CrossBackendSplitter(
        backend_for_collection=backend_for_collection, always_split=True
    )
    pjob: PartitionedJob = splitter.split({"process_graph": process_graph})
    _log.info(f"Partitioned job: {pjob!r}")

    with TimingLogger(title="Running partitioned job", logger=_log):
        run_info = run_partitioned_job(pjob=pjob, connection=connection)
    print(f"Run info of subjobs: {run_info}")


if __name__ == "__main__":
    main()
