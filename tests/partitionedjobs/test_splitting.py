import pytest

from openeo_aggregator.backend import AggregatorBackendImplementation, AggregatorProcessing, AggregatorCollectionCatalog
from openeo_aggregator.partitionedjobs.splitting import TileGridSplitter, TileGrid, FlimsySplitter


def test_tile_grid_spec_from_string():
    assert TileGrid.from_string("wgs84-1degree") == TileGrid(crs_type="wgs84", size=1, unit="degree")
    assert TileGrid.from_string("utm-10km") == TileGrid(crs_type="utm", size=10, unit="km")


def test_flimsy_splitter(multi_backend_connection):
    splitter = FlimsySplitter(processing=AggregatorProcessing(
        backends=multi_backend_connection,
        catalog=AggregatorCollectionCatalog(backends=multi_backend_connection)
    ))
    process = {"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}}
    pjob = splitter.split(process)
    assert len(pjob.subjobs) == 1
    assert pjob.subjobs[0].process_graph == {
        "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
    }


class TestTileGridSplitter:

    @staticmethod
    @pytest.fixture
    def backend_implementation(
            multi_backend_connection, config, requests_mock, backend1
    ) -> AggregatorBackendImplementation:
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={})

        return AggregatorBackendImplementation(backends=multi_backend_connection, config=config)

    @pytest.mark.parametrize(["west", "south", "tile_grid", "expected_extent"], [
        # >>> from pyproj import Transformer
        # >>> Transformer.from_crs("epsg:4326", "epsg:32631", always_xy=True).transform(5, 51)
        # (640333.2963383198, 5651728.68267166)
        (
                5, 51, "utm-100km",
                {"west": 600_000, "south": 5_600_000, "east": 700_000, "north": 5_700_000, "crs": "epsg:32631"}
        ),
        (
                5, 51, "utm-20km",
                {"west": 640_000, "south": 5_640_000, "east": 660_000, "north": 5_660_000, "crs": "epsg:32631"}
        ),
        (
                5, 51, "utm-10km",
                {"west": 640_000, "south": 5_650_000, "east": 650_000, "north": 5_660_000, "crs": "epsg:32631"}
        ),
        (
                5, 51, "wgs84-1degree",
                {"west": 5, "south": 51, "east": 6, "north": 52, "crs": "epsg:4326"}
        ),
        # >>> Transformer.from_crs("epsg:4326", "epsg:32633", always_xy=True).transform(12.3, 45.6)
        # (289432.90485397115, 5053152.380961399)
        (
                12.3, 45.6, "utm-20km",
                {"west": 280_000, "south": 5_040_000, "east": 300_000, "north": 5_060_000, "crs": "epsg:32633"}
        ),
        # >>> Transformer.from_crs("epsg:4326", "epsg:32724", always_xy=True).transform(-42, -5)
        # (167286.20126155682, 9446576.013116669)
        (
                -42, -5, "utm-10km",
                {"west": 160_000, "south": 9_440_000, "east": 170_000, "north": 9_450_000, "crs": "epsg:32724"}
        ),
    ])
    def test_simple_small_coverage(self, backend_implementation,  tile_grid, west, south, expected_extent):
        """load_collection with very small spatial extent that should only cover one tile"""
        splitter = TileGridSplitter(backend_implementation=backend_implementation)
        process = {"process_graph": {
            "lc": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": {
                    "west": west, "south": south,
                    "east": west + 0.001, "north": south + 0.001
                }},
            },
            "sr": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "GTiff"},
                "result": True
            },
        }}

        pjob = splitter.split(process=process, job_options={"tile_grid": tile_grid})
        assert len(pjob.subjobs) == 1
        new_process_graph = pjob.subjobs[0].process_graph
        assert len(new_process_graph) == 3
        new_node_id, = set(new_process_graph.keys()).difference(process["process_graph"].keys())
        assert new_process_graph["lc"] == process["process_graph"]["lc"]
        assert new_process_graph[new_node_id] == {
            "process_id": "filter_bbox",
            "arguments": {"data": {"from_node": "lc"}, "extent": expected_extent},
        }
        assert new_process_graph["sr"] == {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": new_node_id}, "format": "GTiff"},
            "result": True
        }
