from openeo_aggregator.partitionedjobs import SubJob
from openeo_aggregator.partitionedjobs.crossbackend import CrossBackendSplitter


class TestCrossBackendSplitter:
    def test_simple(self):
        process_graph = {
            "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}
        }
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: "foo")
        res = splitter.split({"process_graph": process_graph})

        assert res.subjobs == {"main": SubJob(process_graph, backend_id=None)}
        assert res.dependencies == {"main": []}

    def test_basic(self):
        process_graph = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
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
        splitter = CrossBackendSplitter(
            backend_for_collection=lambda cid: cid.split("_")[0]
        )
        res = splitter.split({"process_graph": process_graph})

        assert res.subjobs == {
            "main": SubJob(
                process_graph={
                    "lc1": {
                        "process_id": "load_collection",
                        "arguments": {"id": "B1_NDVI"},
                    },
                    "lc2": {
                        "process_id": "load_result",
                        "arguments": {"id": "_placeholder:B2:lc2"},
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
                },
                backend_id="B1",
            ),
            "B2:lc2": SubJob(
                process_graph={
                    "lc2": {
                        "process_id": "load_collection",
                        "arguments": {"id": "B2_FAPAR"},
                    },
                    "sr1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                        "result": True,
                    },
                },
                backend_id="B2",
            ),
        }
        assert res.dependencies == {"main": ["B2:lc2"]}
