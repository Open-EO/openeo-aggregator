import itertools
import time

import pytest

from openeo_aggregator.backend import AggregatorCollectionCatalog, AggregatorProcessing, \
    AggregatorBackendImplementation, _InternalCollectionMetadata, JobIdMapping
from openeo_aggregator.connection import MultiBackendConnection
from openeo_driver.errors import OpenEOApiException, CollectionNotFoundException, JobNotFoundException
from openeo_driver.users.oidc import OidcProvider


class TestAggregatorBackendImplementation:

    def test_oidc_providers(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        providers = implementation.oidc_providers()
        assert providers == [
            OidcProvider(id='egi', issuer='https://egi.test', title='EGI'),
            OidcProvider(id='x-agg', issuer='https://x.test', title='X (agg)'),
            OidcProvider(id='y-agg', issuer='https://y.test', title='Y (agg)'),
            OidcProvider(id='z-agg', issuer='https://z.test', title='Z (agg)'),
        ]

    def test_file_formats_simple(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}}
        }
        requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        requests_mock.get(backend2 + "/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == just_geotiff

    def test_file_formats_caching(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}}
        }
        mock1 = requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        mock2 = requests_mock.get(backend2 + "/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == just_geotiff
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        _ = implementation.file_formats()
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        implementation._cache.flush_all()
        _ = implementation.file_formats()
        assert mock1.call_count == 2
        assert mock2.call_count == 2

    def test_file_formats_merging(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/file_formats", json={
            "input": {
                "GeoJSON": {"gis_data_types": ["vector"], "parameters": {}}},
            "output": {
                "CSV": {"gis_data_types": ["raster"], "parameters": {}, "title": "Comma Separated Values"},
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {"ZLEVEL": {"type": "string", "default": "6"}, },
                    "title": "GeoTiff"
                },
                "JSON": {"gis_data_types": ["raster"], "parameters": {}},
                "netCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        })
        requests_mock.get(backend2 + "/file_formats", json={
            "input": {
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
            },
            "output": {
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
                "NetCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        })
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GeoJSON": {"gis_data_types": ["vector"], "parameters": {}},
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
            },
            "output": {
                "CSV": {"gis_data_types": ["raster"], "parameters": {}, "title": "Comma Separated Values"},
                "GTiff": {
                    "gis_data_types": ["raster"],
                    # TODO: merge parameters of backend1 and backend2?
                    "parameters": {"ZLEVEL": {"type": "string", "default": "6"}, },
                    "title": "GeoTiff"
                },
                "JSON": {"gis_data_types": ["raster"], "parameters": {}},
                "netCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        }


class TestInternalCollectionMetadata:

    def test_get_set_backends_for_collection(self):
        internal = _InternalCollectionMetadata()
        internal.set_backends_for_collection("S2", ["b1", "b3"])
        internal.set_backends_for_collection("S1", {"b1": 1, "b2": 2}.keys())
        assert internal.get_backends_for_collection("S2") == ["b1", "b3"]
        assert internal.get_backends_for_collection("S1") == ["b1", "b2"]
        with pytest.raises(CollectionNotFoundException):
            internal.get_backends_for_collection("S2222")

    def test_list_backends_for_collections(self):
        internal = _InternalCollectionMetadata()
        internal.set_backends_for_collection("S2", ["b1", "b3"])
        internal.set_backends_for_collection("S1", ["b1", "b2"])
        assert sorted(internal.list_backends_per_collection()) == [
            ("S1", ["b1", "b2"]),
            ("S2", ["b1", "b3"]),
        ]


class TestAggregatorCollectionCatalog:

    def test_get_all_metadata_simple(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [{"id": "S2"}, {"id": "S3"}]

    def test_get_all_metadata_common_collections_minimal(
            self, multi_backend_connection, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {"id": "S3"},
            {
                "id": "S4", "description": "S4", "title": "S4",
                "stac_version": "0.9.0",
                "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
                "license": "proprietary",
                "summaries": {"provider:backend": ["b1", "b2"]},
                "links": [],
            },
            {"id": "S5"},
        ]

    def test_get_all_metadata_common_collections_merging(
            self, multi_backend_connection, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{
            "id": "S4",
            "stac_version": "0.9.0",
            "title": "B1's S4", "description": "This is B1's S4",
            "keywords": ["S4", "B1"],
            "version": "1.2.3",
            "license": "MIT",
            "providers": [{"name": "ESA", "roles": ["producer"]}],
            "extent": {
                "spatial": {"bbox": [[-10, 20, 30, 50]]},
                "temporal": {"interval": [["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"]]}
            },
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B01", "B02"]},
                "x": {"type": "spatial", "axis": "x"}
            },
            "links": [
                {"rel": "license", "href": "https://spdx.org/licenses/MIT.html"},
            ],
        }]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{
            "id": "S4",
            "stac_version": "0.9.0",
            "title": "B2's S4", "description": "This is B2's S4",
            "keywords": ["S4", "B2"],
            "version": "2.4.6",
            "license": "Apache-1.0",
            "providers": [{"name": "ESA", "roles": ["licensor"]}],
            "extent": {
                "spatial": {"bbox": [[-20, -20, 40, 40]]},
                "temporal": {"interval": [["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]]}
            },
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B01", "B02"]},
                "y": {"type": "spatial", "axis": "y"}
            },
            "links": [
                {"rel": "license", "href": "https://spdx.org/licenses/Apache-1.0.html"},
            ],
        }]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                "id": "S4",
                "title": "B1's S4",
                "description": "This is B1's S4",
                "keywords": ["S4", "B1", "B2"],
                "version": "2.4.6",
                "stac_version": "0.9.0",
                "extent": {
                    "spatial": {"bbox": [[-10, 20, 30, 50], [-20, -20, 40, 40]]},
                    "temporal": {"interval": [["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"],
                                              ["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]]}},
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": ["B01", "B02"]},
                    "x": {"type": "spatial", "axis": "x"}
                },
                "license": "various",
                "providers": [{"name": "ESA", "roles": ["producer"]}, {"name": "ESA", "roles": ["licensor"]}],
                "summaries": {"provider:backend": ["b1", "b2"]},
                "links": [
                    {"rel": "license", "href": "https://spdx.org/licenses/MIT.html"},
                    {"rel": "license", "href": "https://spdx.org/licenses/Apache-1.0.html"},
                ],
            },
        ]

    def test_get_best_backend_for_collections_basic(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)

        with pytest.raises(OpenEOApiException, match="Empty collection set given"):
            catalog.get_backend_candidates_for_collections([])

        assert catalog.get_backend_candidates_for_collections(["S3"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4"]) == ["b1", "b2"]
        assert catalog.get_backend_candidates_for_collections(["S5"]) == ["b2"]
        assert catalog.get_backend_candidates_for_collections(["S3", "S4"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4", "S5"]) == ["b2"]

        with pytest.raises(OpenEOApiException, match="Collections across multiple backends"):
            catalog.get_backend_candidates_for_collections(["S3", "S4", "S5"])

    def test_get_collection_metadata_basic(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend2 + "/collections/S3", json={"id": "S3", "title": "b2's S3"})

        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {"id": "S2", "title": "b1's S2"}
        metadata = catalog.get_collection_metadata("S3")
        assert metadata == {"id": "S3", "title": "b2's S3"}

        with pytest.raises(CollectionNotFoundException):
            catalog.get_collection_metadata("S5")

    def test_get_collection_metadata_merging(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "stac_version": "0.9.0",
            "id": "S2",
            "description": "S2",
            "title": "b1's S2",
            "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
            "license": "proprietary",
            "summaries": {"provider:backend": ["b1", "b2"]},
            "links": [],
        }

    def test_get_collection_metadata_merging_with_error(
            self, multi_backend_connection, backend1, backend2, requests_mock,
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", status_code=500)
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "title": "b2's S2",
        }
        # TODO: test that caching of result is different from merging without error? (#2)

    # TODO tests for caching of collection metadata

    def test_generate_backend_constraint_callables(self):
        callables = AggregatorCollectionCatalog.generate_backend_constraint_callables([
            {"eq": {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "b1"}, "result": True}},
            {"eq": {"process_id": "neq", "arguments": {"x": {"from_parameter": "value"}, "y": "b2"}, "result": True}},
        ])
        equals_b1, differs_from_b2 = callables
        assert equals_b1("b1") is True
        assert equals_b1("b2") is False
        assert equals_b1("b3") is False
        assert differs_from_b2("b1") is True
        assert differs_from_b2("b2") is False
        assert differs_from_b2("b3") is True


class TestJobIdMapping:

    def test_get_aggregator_job_id(self):
        assert JobIdMapping.get_aggregator_job_id(
            backend_job_id="j0bId-f00b6r", backend_id="vito"
        ) == "vito-j0bId-f00b6r"

    def test_parse_aggregator_job_id(self, multi_backend_connection):
        assert JobIdMapping.parse_aggregator_job_id(
            backends=multi_backend_connection, aggregator_job_id="b1-j000b"
        ) == ("j000b", "b1")
        assert JobIdMapping.parse_aggregator_job_id(
            backends=multi_backend_connection, aggregator_job_id="b2-b6tch-j0b-o123423"
        ) == ("b6tch-j0b-o123423", "b2")

    def test_parse_aggregator_job_id_fail(self, multi_backend_connection):
        with pytest.raises(JobNotFoundException):
            JobIdMapping.parse_aggregator_job_id(
                backends=multi_backend_connection, aggregator_job_id="b3-b6tch-j0b-o123423"
            )


class TestAggregatorProcessing:

    def test_get_process_registry(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]

    def test_get_process_registry_parameter_differences(
            self, multi_backend_connection, backend1, backend2,
            requests_mock
    ):
        requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "array"}]},
        ]})
        requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "values"}]},
        ]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "array"}]},
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]
