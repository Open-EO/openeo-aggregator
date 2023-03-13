import collections
import logging
from pprint import pprint
from typing import Callable, Dict, Tuple

from openeo_aggregator.partitionedjobs import SubJob

_log = logging.getLogger(__name__)


def cross_backend_split(
    process_graph: dict,
    backend_for_collection: Callable[[str], str],
) -> Tuple[SubJob, Dict[str, SubJob]]:
    """
    Split a process graph for cross-back-end processing

    :param process_graph: flat dict representation of a process graph
    :param backend_for_collection: function that determines backend for a given collection id
    :return:
    """
    # Extract necessary back-ends from `load_collection` usage
    backend_usage = collections.Counter(
        backend_for_collection(node["arguments"]["id"])
        for node in process_graph.values()
        if node["process_id"] == "load_collection"
    )
    _log.info(f"Extracted backend usage from `load_collection` nodes: {backend_usage}")

    primary_backend = backend_usage.most_common(1)[0][0]
    secondary_backends = {b for b in backend_usage if b != primary_backend}
    _log.info(f"Backend split: {primary_backend=} {secondary_backends=}")

    primary_pg = SubJob(process_graph={}, backend_id=primary_backend)
    secondary_pgs: Dict[str, SubJob] = {}
    for node_id, node in process_graph.items():
        if node["process_id"] == "load_collection":
            bid = backend_for_collection(node["arguments"]["id"])
            if bid == primary_backend:
                primary_pg.process_graph[node_id] = node
            else:
                # New secondary pg
                pg = {
                    node_id: node,
                    "sr1": {
                        # TODO: other/better choices for save_result format (e.g. based on backend support)?
                        "process_id": "save_result",
                        "arguments": {"format": "NetCDF"},
                    },
                }
                dependency_id = node_id
                secondary_pgs[dependency_id] = SubJob(process_graph=pg, backend_id=bid)
                # Link to primary pg with load_result
                primary_pg.process_graph[node_id] = {
                    # TODO: encapsulate this placeholder process/id better?
                    "process_id": "load_result",
                    "arguments": {"id": f"placeholder:{dependency_id}"},
                }
        else:
            primary_pg.process_graph[node_id] = node

    return primary_pg, secondary_pgs


def main():
    process_graph = {
        "lc1": {"process_id": "load_collection", "arguments": {"id": "VITO_1"}},
        "lc2": {"process_id": "load_collection", "arguments": {"id": "SH_1"}},
        "mc1": {
            "process_id": "merge_cubes",
            "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
        },
        "sr1": {
            "process_id": "save_result",
            "arguments": {"format": "NetCDF"},
        },
    }
    print("Original PG:")
    pprint(process_graph)

    split = cross_backend_split(
        process_graph, backend_for_collection=lambda cid: cid.split("_")[0]
    )
    print("Cross-backend split:")
    pprint(split)


if __name__ == "__main__":
    main()
