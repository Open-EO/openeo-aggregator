import datetime as dt
import logging
import re

import pytest
from dirty_equals import IsPartialDict
from openeo.rest import OpenEoApiError, OpenEoApiPlainError, OpenEoRestError
from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import (
    CollectionNotFoundException,
    JobNotFoundException,
    OpenEOApiException,
    ProcessGraphInvalidException,
    ProcessGraphMissingException,
    ServiceNotFoundException,
    ServiceUnsupportedException,
)
from openeo_driver.testing import DictSubSet
from openeo_driver.users.auth import HttpAuthHandler
from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.backend import (
    AggregatorBackendImplementation,
    AggregatorCollectionCatalog,
    AggregatorProcessing,
    AggregatorSecondaryServices,
    CollectionAllowList,
    JobIdMapping,
    _InternalCollectionMetadata,
)
from openeo_aggregator.caching import DictMemoizer
from openeo_aggregator.config import ProcessAllowed
from openeo_aggregator.testing import clock_mock, config_overrides

# TODO: "backend.py" should not really be authentication-aware, can we eliminate these constants
#       and move the tested functionality to test_views.py?
#       Also see https://github.com/Open-EO/openeo-aggregator/pull/79#discussion_r1022018851
TEST_USER = "Mr.Test"
TEST_USER_BEARER_TOKEN = "basic//" + HttpAuthHandler.build_basic_access_token(user_id=TEST_USER)
TEST_USER_AUTH_HEADER = {"Authorization": "Bearer " + TEST_USER_BEARER_TOKEN}


class TestAggregatorBackendImplementation:
    def test_oidc_providers(self, multi_backend_connection, backend1, backend2):
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        providers = implementation.oidc_providers()
        assert providers == [
            OidcProvider(id="egi", issuer="https://egi.test", title="EGI"),
            OidcProvider(id="x-agg", issuer="https://x.test", title="X (agg)"),
            OidcProvider(id="y-agg", issuer="https://y.test", title="Y (agg)"),
            OidcProvider(id="z-agg", issuer="https://z.test", title="Z (agg)"),
        ]

    def test_oidc_providers_new_config_support(self, multi_backend_connection, backend1, backend2):
        """Test for new AggregatorBackendConfig.oidc_providers support."""
        with config_overrides(oidc_providers=[OidcProvider(id="aagg", issuer="https://aagg.test", title="Aagg)")]):
            implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        providers = implementation.oidc_providers()
        assert providers == [
            OidcProvider(id="aagg", issuer="https://aagg.test", title="Aagg)"),
        ]

    def test_file_formats_simple(self, multi_backend_connection, backend1, backend2, requests_mock):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
        }
        requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        requests_mock.get(backend2 + "/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": ["b1", "b2"],
                }
            },
            "output": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": ["b1", "b2"],
                }
            },
            "federation:backends": ["b1", "b2"],
            "federation:missing": [],
        }

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {dict}),
            ({"memoizer": {"type": "jsondict"}}, {bytes}),
        ],
    )
    def test_file_formats_caching(
        self, multi_backend_connection, backend1, backend2, requests_mock, overrides, expected_cache_types
    ):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
        }
        mock1 = requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        mock2 = requests_mock.get(backend2 + "/file_formats", json=just_geotiff)

        with config_overrides(**overrides):
            implementation = AggregatorBackendImplementation(backends=multi_backend_connection)

        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": ["b1", "b2"],
                }
            },
            "output": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": ["b1", "b2"],
                }
            },
            "federation:backends": ["b1", "b2"],
            "federation:missing": [],
        }
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        _ = implementation.file_formats()
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        with clock_mock(offset=100):
            _ = implementation.file_formats()
        assert mock1.call_count == 2
        assert mock2.call_count == 2

        assert isinstance(implementation._memoizer, DictMemoizer)
        cache_dump = implementation._memoizer.dump(values_only=True)
        assert len(cache_dump) == 1
        assert set(type(v) for v in cache_dump) == expected_cache_types

    def test_file_formats_merging(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(
            backend1 + "/file_formats",
            json={
                "input": {"GeoJSON": {"gis_data_types": ["vector"], "parameters": {}}},
                "output": {
                    "CSV": {"gis_data_types": ["raster"], "parameters": {}, "title": "Comma Separated Values"},
                    "GTiff": {
                        "gis_data_types": ["raster"],
                        "parameters": {
                            "ZLEVEL": {"type": "string", "default": "6"},
                        },
                        "title": "GeoTiff",
                    },
                    "JSON": {"gis_data_types": ["raster"], "parameters": {}},
                    "netCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
                },
            },
        )
        requests_mock.get(
            backend2 + "/file_formats",
            json={
                "input": {
                    "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
                },
                "output": {
                    "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
                    "NetCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
                },
            },
        )
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GeoJSON": {
                    "gis_data_types": ["vector"],
                    "parameters": {},
                    "federation:backends": ["b1"],
                },
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": ["b2"],
                },
            },
            "output": {
                "CSV": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "Comma Separated Values",
                    "federation:backends": ["b1"],
                },
                "GTiff": {
                    "gis_data_types": ["raster"],
                    # TODO: merge parameters of backend1 and backend2?
                    "parameters": {
                        "ZLEVEL": {"type": "string", "default": "6"},
                    },
                    "title": "GeoTiff",
                    "federation:backends": ["b1", "b2"],
                },
                "JSON": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "federation:backends": ["b1"],
                },
                "netCDF": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "netCDF",
                    "federation:backends": ["b1", "b2"],
                },
            },
            "federation:backends": ["b1", "b2"],
            "federation:missing": [],
        }

    @pytest.mark.parametrize(
        ["fail_capabilities", "fail_file_formats", "expected_backends", "expected_missing"],
        [
            (False, False, ["b1", "b2"], []),
            (True, False, ["b1"], ["b2"]),
            (False, True, ["b1"], ["b2"]),
            (True, True, ["b1"], ["b2"]),
        ],
    )
    def test_file_formats_missing_backends(
        self,
        multi_backend_connection,
        backend1,
        backend2,
        requests_mock,
        fail_capabilities,
        fail_file_formats,
        expected_backends,
        expected_missing,
    ):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
        }
        requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        if fail_capabilities:
            requests_mock.get(f"{backend2}/", status_code=404, text="nope")
        if fail_file_formats:
            requests_mock.get(f"{backend2}/file_formats", status_code=404, text="nope")
        else:
            requests_mock.get(f"{backend2}/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": expected_backends,
                }
            },
            "output": {
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "title": "GeoTiff",
                    "federation:backends": expected_backends,
                }
            },
            "federation:backends": expected_backends,
            "federation:missing": expected_missing,
        }


class TestAggregatorSecondaryServices:

    # TODO: most tests here (the ones that do flask app stuff and auth)
    #       belong under test_views.py

    SERVICE_TYPES_ONLT_WMTS = {
        "WMTS": {
            "configuration": {
                "colormap": {
                    "default": "YlGn",
                    "description": "The colormap to apply to single band layers",
                    "type": "string",
                },
                "version": {
                    "default": "1.0.0",
                    "description": "The WMTS version to use.",
                    "enum": ["1.0.0"],
                    "type": "string",
                },
            },
            "links": [],
            "process_parameters": [],
            "title": "Web Map Tile Service",
        }
    }

    def test_get_supporting_backend_ids_none_supported(self, multi_backend_connection, catalog):
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        actual_supported_backends = implementation.get_supporting_backend_ids()
        assert actual_supported_backends == []

    def test_get_supporting_backend_ids_all_supported(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        actual_supported_backends = implementation.get_supporting_backend_ids()
        assert actual_supported_backends == ["b1", "b2"]

    def test_get_supporting_backend_ids_only_one_supported(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=False))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        actual_supported_backends = implementation.get_supporting_backend_ids()
        assert actual_supported_backends == ["b2"]

    def test_service_types_simple(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        """Given 2 backends and only 1 backend has a single service type, then the aggregator
        returns that 1 service type's metadata.
        """
        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))
        single_service_type = self.SERVICE_TYPES_ONLT_WMTS
        requests_mock.get(backend1 + "/service_types", json=single_service_type)
        requests_mock.get(backend2 + "/service_types", json={})

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        service_types = implementation.service_types()
        assert service_types == single_service_type

    def test_service_types_simple_cached(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        """Scenario: The service_types call is cached:
        When we get the service types several times, the second call that happens before the cache expires,
        doesn't hit the backend.
        But the third call that happens after the cache has expired does hit the backend again.
        """
        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))
        # Just need one service type for the test.
        single_service_type = self.SERVICE_TYPES_ONLT_WMTS
        mock_be1 = requests_mock.get(backend1 + "/service_types", json=single_service_type)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        service_types = implementation.service_types()
        assert mock_be1.call_count == 1
        assert service_types == single_service_type

        # Second call happens before the cache item expires: it should not query the backend.
        service_types = implementation.service_types()
        assert mock_be1.call_count == 1
        assert service_types == single_service_type

        # Third call happens when the cached item has expired: it should query the backend.
        with clock_mock(offset=100):
            service_types = implementation.service_types()
            assert mock_be1.call_count == 2
            assert service_types == single_service_type

    def test_service_types_skips_unsupported_backend(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        """Given 2 backends and only 1 backend support secondary services, as states in its capabilities,
        when the aggregator fulfills a request to list the service types,
        then the aggregator skips the unsupported backend.
        """
        # We are testing that the aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        # We want to verify that the capabilities are actually queried.
        mock_b1_capabilities = requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=False))
        mock_b2_capabilities = requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))

        single_service_type = self.SERVICE_TYPES_ONLT_WMTS
        # Backend 1 does support secondary services
        mock_b1_service_types = requests_mock.get(backend1 + "/service_types", status_code=500)
        mock_b2_service_types = requests_mock.get(backend2 + "/service_types", json=single_service_type)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        service_types = implementation.service_types()
        assert service_types == single_service_type

        assert mock_b1_capabilities.called
        assert mock_b2_capabilities.called
        assert mock_b2_service_types.called
        assert not mock_b1_service_types.called

    def test_service_types_multiple_backends(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        mbldr,
    ):
        """Given 2 backends with each 1 service type, then the aggregator lists both service types."""

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))
        service_type_1 = {
            "WMTS": {
                "configuration": {
                    "colormap": {
                        "default": "YlGn",
                        "description": "The colormap to apply to single band layers",
                        "type": "string",
                    },
                    "version": {
                        "default": "1.0.0",
                        "description": "The WMTS version to use.",
                        "enum": ["1.0.0"],
                        "type": "string",
                    },
                },
                "links": [],
                "process_parameters": [],
                "title": "Web Map Tile Service",
            }
        }
        service_type_2 = {
            "WMS": {"title": "OGC Web Map Service", "configuration": {}, "process_parameters": [], "links": []}
        }
        requests_mock.get(backend1 + "/service_types", json=service_type_1)
        requests_mock.get(backend2 + "/service_types", json=service_type_2)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        actual_service_types = implementation.service_types()

        expected_service_types = dict(service_type_1)
        expected_service_types.update(service_type_2)
        assert actual_service_types == expected_service_types

    def test_service_types_warns_about_duplicate_service(
        self,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        caplog,
        mbldr,
    ):
        """
        Given 2 backends which have conflicting service types,
        then the aggregator lists only the service type from the first backend
        and it logs a warning about the conflicting types.
        """

        caplog.set_level(logging.WARNING)
        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(secondary_services=True))
        service_type_1 = {
            "WMS": {"title": "OGC Web Map Service", "configuration": {}, "process_parameters": [], "links": []}
        }
        service_type_2 = {
            "WMS": {
                "title": "A duplicate OGC Web Map Service",
                "configuration": {},
                "process_parameters": [],
                "links": [],
            }
        }
        requests_mock.get(backend1 + "/service_types", json=service_type_1)
        requests_mock.get(backend2 + "/service_types", json=service_type_2)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        actual_service_types = implementation.service_types()

        # There were duplicate service types:
        # Therefore it should find only one service type, and the log should contain a warning.
        expected_service_types = dict(service_type_1)
        assert actual_service_types == expected_service_types

        expected_log_message = (
            'Conflicting secondary service types: "WMS" is present in more than one backend, '
            + "already found in backend: b1"
        )
        assert expected_log_message in caplog.text

    @pytest.fixture
    def service_metadata_wmts_foo(self):
        return ServiceMetadata(
            id="wmts-foo",
            process={"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}},
            url="https://oeo.net/wmts/foo",
            type="WMTS",
            enabled=True,
            configuration={"version": "0.5.8"},
            attributes={},
            title="Test WMTS service",
            created=dt.datetime(2020, 4, 9, 15, 5, 8),
        )

    @pytest.fixture
    def service_metadata_wms_bar(self):
        return ServiceMetadata(
            id="wms-bar",
            process={"process_graph": {"bar": {"process_id": "bar", "arguments": {}}}},
            url="https://oeo.net/wms/bar",
            type="WMS",
            enabled=True,
            configuration={"version": "0.5.8"},
            attributes={},
            title="Test WMS service",
            created=dt.datetime(2022, 2, 1, 13, 30, 3),
        )

    def test_service_info_succeeds(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        backend2,
        requests_mock,
        service_metadata_wmts_foo,
        service_metadata_wms_bar,
    ):
        """When it gets a correct service ID, it returns the expected ServiceMetadata."""
        json_wmts_foo = service_metadata_wmts_foo.prepare_for_json()
        json_wms_bar = service_metadata_wms_bar.prepare_for_json()
        requests_mock.get(backend1 + "/services/wmts-foo", json=json_wmts_foo)
        requests_mock.get(backend2 + "/services/wms-bar", json=json_wms_bar)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        # Check the expected metadata on *both* of the services.
        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            actual_service1 = implementation.service_info(user_id=TEST_USER, service_id="b1-wmts-foo")

            json = dict(json_wmts_foo)
            json["id"] = "b1-" + json["id"]
            expected_service1 = ServiceMetadata.from_dict(json)

            assert actual_service1 == expected_service1

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            actual_service2 = implementation.service_info(user_id=TEST_USER, service_id="b2-wms-bar")

            json = dict(json_wms_bar)
            json["id"] = "b2-" + json["id"]
            expected_service2 = ServiceMetadata.from_dict(json)

            assert actual_service2 == expected_service2

    def test_service_info_wrong_backend_id(
        self, flask_app, multi_backend_connection, catalog, backend1, requests_mock, service_metadata_wmts_foo
    ):
        """When it gets a non-existent service ID, it raises a ServiceNotFoundException."""

        requests_mock.get(backend1 + "/services/wmts-foo", json=service_metadata_wmts_foo.prepare_for_json())
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.service_info(user_id=TEST_USER, service_id="backenddoesnotexist-wtms-foo")

    def test_service_info_wrong_service_id(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        requests_mock,
    ):
        """When it gets a non-existent service ID, it raises a ServiceNotFoundException."""

        requests_mock.get(backend1 + "/services/service-does-not-exist", status_code=404)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.service_info(user_id=TEST_USER, service_id="b1-service-does-not-exist")

        assert requests_mock.called

    def test_create_service_succeeds(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        requests_mock,
        mbldr,
    ):
        """When it gets a correct params for a new service, it successfully creates it."""

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))

        # Set up responses for creating the service in backend 1
        backend_service_id = "wmts-foo"
        # The aggregator should prepend the service_id with the backend_id
        expected_service_id = "b1-wmts-foo"

        location_backend_1 = backend1 + "/services/" + backend_service_id
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        requests_mock.post(
            backend1 + "/services",
            headers={"OpenEO-Identifier": backend_service_id, "Location": location_backend_1},
            status_code=201,
        )
        requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            actual_openeo_id = implementation.create_service(
                user_id=TEST_USER,
                process_graph=process_graph,
                service_type="WMTS",
                api_version="1.0.0",
                configuration={},
            )
            assert actual_openeo_id == expected_service_id

    def test_create_service_raises_serviceunsupportedexception(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        requests_mock,
        mbldr,
    ):
        """When it gets a request for a service type but no backend supports this service type, it raises ServiceUnsupportedException."""

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        mock_get_capabilities = requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))

        # At least 1 service type must be present.
        # We don't want test to succeed erroneously simply because there are no services at all.
        mock_service_types = requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        non_existent_service_id = "b1-doesnotexist"
        # Check that this requests_mock does not get called.
        location_backend_1 = backend1 + "/services/" + non_existent_service_id
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        mock_post = requests_mock.post(
            backend1 + "/services",
            headers={"OpenEO-Identifier": "wmts-foo", "Location": location_backend_1},
            status_code=201,
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceUnsupportedException):
                implementation.create_service(
                    user_id=TEST_USER,
                    process_graph=process_graph,
                    service_type="does-not-exist",
                    api_version="1.0.0",
                    configuration={},
                )
            assert not mock_post.called
            # The backend that we are using should support the GET /service_types endpoint,
            # or it would also raise ServiceUnsupportedException for a different reason:
            # finding no backends that support any SecondaryServices in general.
            assert mock_get_capabilities.called
            assert mock_service_types.called

    @pytest.mark.parametrize(
        "exception_factory",
        [
            lambda m: OpenEoApiError(http_status_code=500, code="Internal", message=m),
            OpenEoRestError,
        ],
    )
    def test_create_service_backend_raises_openeoapiexception(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        requests_mock,
        exception_factory,
        mbldr,
    ):
        """When the backend raises a general exception the aggregator raises an OpenEOApiException."""

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        mock_get_capabilities = requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))

        # Set up responses for creating the service in backend 1:
        # This time the backend raises an error, one that will be reported as a OpenEOApiException.
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        mock_post = requests_mock.post(
            backend1 + "/services",
            exc=exception_factory("Some server error"),
        )
        mock_service_types = requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(OpenEOApiException):
                implementation.create_service(
                    user_id=TEST_USER,
                    process_graph=process_graph,
                    service_type="WMTS",
                    api_version="1.0.0",
                    configuration={},
                )
            assert mock_get_capabilities.called
            assert mock_service_types.called
            assert mock_post.called

    @pytest.mark.parametrize(
        "exception_class", [ProcessGraphMissingException, ProcessGraphInvalidException, ServiceUnsupportedException]
    )
    def test_create_service_backend_reraises(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
        backend1,
        requests_mock,
        exception_class,
        mbldr,
    ):
        """When the backend raises certain exception types of which we know it indicates client error / bad input data,
        then the aggregator re-raises that exception.
        """

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        mock_get_capabilities = requests_mock.get(backend1 + "/", json=mbldr.capabilities(secondary_services=True))

        # Set up responses for creating the service in backend 1
        # This time the backend raises an error, one that will simply be re-raised/passed on as it is.
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        mock_post = requests_mock.post(backend1 + "/services", exc=exception_class("Some server error"))
        mock_get_service_types = requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            # These exception types should be re-raised, not become an OpenEOApiException.
            with pytest.raises(exception_class):
                implementation.create_service(
                    user_id=TEST_USER,
                    process_graph=process_graph,
                    service_type="WMTS",
                    api_version="1.0.0",
                    configuration={},
                )
            assert mock_get_service_types.called
            assert mock_get_capabilities.called
            assert mock_post.called

    def test_remove_service_succeeds(self, flask_app, multi_backend_connection, catalog, backend1, requests_mock):
        """When remove_service is called with an existing service ID, it removes service and returns HTTP 204."""

        mock_delete = requests_mock.delete(backend1 + "/services/wmts-foo", status_code=204)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            implementation.remove_service(user_id=TEST_USER, service_id="b1-wmts-foo")

            # Make sure the aggregator asked the backend to remove the service.
            assert mock_delete.called

    def test_remove_service_but_backend_id_not_found(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
    ):
        """When the backend ID/prefix does not exist then the aggregator raises an ServiceNotFoundException."""

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        # Case 1: the backend doesn't even exist
        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.remove_service(user_id=TEST_USER, service_id="doesnotexist-wmts-foo")

    def test_remove_service_but_service_id_not_found(
        self, flask_app, multi_backend_connection, catalog, backend1, requests_mock
    ):
        """When the service ID does not exist for the specified backend then the aggregator raises an ServiceNotFoundException."""

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        # The backend exists but the service ID does not.
        mock_delete1 = requests_mock.delete(backend1 + "/services/doesnotexist", status_code=404)
        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.remove_service(user_id=TEST_USER, service_id="b1-doesnotexist")

            # This should have tried to delete it on the backend so the mock must be called.
            assert mock_delete1.called

    def test_remove_service_backend_response_is_an_error_status(
        self, flask_app, multi_backend_connection, catalog, backend1, requests_mock
    ):
        """When the backend response is an HTTP error status then the aggregator raises an OpenEoApiError."""

        requests_mock.delete(backend1 + "/services/wmts-foo", status_code=500)
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(OpenEoApiPlainError) as e:
                implementation.remove_service(user_id=TEST_USER, service_id="b1-wmts-foo")

            # If the backend reports HTTP 400/500, we would expect the same status code from the aggregator.
            # TODO: Statement above is an assumption. Is that really what we expect?
            assert e.value.http_status_code == 500

    def test_update_service_succeeds(self, flask_app, multi_backend_connection, catalog, backend1, requests_mock):
        """When it receives an existing service ID and a correct payload, it updates the expected service."""

        mock_patch = requests_mock.patch(
            backend1 + "/services/wmts-foo",
            status_code=204,
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            implementation.update_service(
                user_id=TEST_USER, service_id="b1-wmts-foo", process_graph=process_graph_after
            )

            # Make sure the aggregator asked the backend to remove the service.
            assert mock_patch.called

            # TODO: I am not too sure this json payload is correct. Check with codebases of other backend drivers.
            expected_process = {"process": {"process_graph": process_graph_after}}
            assert mock_patch.last_request.json() == expected_process

    def test_update_service_but_backend_id_does_not_exist(
        self,
        flask_app,
        multi_backend_connection,
        catalog,
    ):
        """When the backend ID/prefix does not exist then the aggregator raises an ServiceNotFoundException."""

        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.update_service(
                    user_id=TEST_USER, service_id="doesnotexist-wmts-foo", process_graph=process_graph_after
                )

    def test_update_service_but_service_id_not_found(
        self, flask_app, multi_backend_connection, catalog, backend1, requests_mock
    ):
        """When the service ID does not exist for the specified backend then the aggregator raises an ServiceNotFoundException."""

        # These requests should not be executed, so check they are not called.
        mock_patch1 = requests_mock.patch(
            backend1 + "/services/doesnotexist",
            status_code=404,
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(ServiceNotFoundException):
                implementation.update_service(
                    user_id=TEST_USER, service_id="b1-doesnotexist", process_graph=process_graph_after
                )

            assert mock_patch1.called

    def test_update_service_backend_response_is_an_error_status(
        self, flask_app, multi_backend_connection, catalog, backend1, requests_mock
    ):
        """When the backend response is an HTTP error status then the aggregator raises an OpenEoApiError."""

        mock_patch = requests_mock.patch(
            backend1 + "/services/wmts-foo",
            status_code=500,
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        implementation = AggregatorSecondaryServices(backends=multi_backend_connection, processing=processing)
        new_process_graph = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}

        with flask_app.test_request_context(headers=TEST_USER_AUTH_HEADER):
            with pytest.raises(OpenEoApiPlainError) as e:
                implementation.update_service(
                    user_id=TEST_USER, service_id="b1-wmts-foo", process_graph=new_process_graph
                )

            assert e.value.http_status_code == 500
            assert mock_patch.called


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


class TestCollectionAllowList:
    def test_basic(self):
        allow_list = CollectionAllowList(
            [
                "foo",
                re.compile("ba+r"),
            ]
        )
        assert allow_list.is_allowed("foo", "b1") is True
        assert allow_list.is_allowed("bar", "b1") is True
        assert allow_list.is_allowed("baaaaar", "b1") is True
        assert allow_list.is_allowed("br", "b1") is False

    def test_allowed_backends(self):
        allow_list = CollectionAllowList(
            [
                "foo",
                {"collection_id": "S2", "allowed_backends": ["b1"]},
            ]
        )
        assert allow_list.is_allowed("foo", "b1") is True
        assert allow_list.is_allowed("foo", "b2") is True
        assert allow_list.is_allowed("S2", "b1") is True
        assert allow_list.is_allowed("S2", "b2") is False

    def test_allowed_backends_field_typo(self):
        with pytest.raises(TypeError, match="unexpected keyword argument 'backends'"):
            _ = CollectionAllowList([{"collection_id": "S2", "backends": ["b1"]}])


@pytest.mark.usefixtures("flask_app")  # Automatically enter flask app context for `url_for` to work
class TestAggregatorCollectionCatalog:
    def test_get_all_metadata_simple(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                "id": "S2",
                "summaries": {"federation:backends": ["b1"]},
            },
            {
                "id": "S3",
                "summaries": {"federation:backends": ["b2"]},
            },
        ]

    def test_get_all_metadata_common_collections_minimal(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(
            backend1 + "/collections",
            json={"collections": [{"id": "S3"}, {"id": "S4"}]},
        )
        requests_mock.get(
            backend2 + "/collections",
            json={"collections": [{"id": "S4"}, {"id": "S5"}]},
        )
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                "id": "S3",
                "summaries": {"federation:backends": ["b1"]},
            },
            {
                "id": "S4",
                "description": "S4",
                "title": "S4",
                "type": "Collection",
                "stac_version": "0.9.0",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "license": "proprietary",
                "summaries": {
                    "federation:backends": ["b1", "b2"],
                    "provider:backend": ["b1", "b2"],
                },
            },
            {
                "id": "S5",
                "summaries": {"federation:backends": ["b2"]},
            },
        ]

    def test_get_all_metadata_common_collections_merging(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(
            backend1 + "/collections",
            json={
                "collections": [
                    {
                        "id": "S4",
                        "stac_version": "0.9.0",
                        "title": "B1's S4",
                        "description": "This is B1's S4",
                        "keywords": ["S4", "B1"],
                        "version": "1.2.3",
                        "license": "MIT",
                        "providers": [{"name": "ESA", "roles": ["producer"]}],
                        "extent": {
                            "spatial": {"bbox": [[-10, 20, 30, 50]]},
                            "temporal": {"interval": [["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"]]},
                        },
                        "cube:dimensions": {
                            "bands": {"type": "bands", "values": ["B01", "B02"]},
                            "x": {"type": "spatial", "axis": "x", "extent": [3, 4]},
                        },
                        "links": [
                            {
                                "rel": "license",
                                "href": "https://spdx.org/licenses/MIT.html",
                            },
                        ],
                    }
                ]
            },
        )
        requests_mock.get(
            backend2 + "/collections",
            json={
                "collections": [
                    {
                        "id": "S4",
                        "stac_version": "0.9.0",
                        "title": "B2's S4",
                        "description": "This is B2's S4",
                        "keywords": ["S4", "B2"],
                        "version": "2.4.6",
                        "license": "Apache-1.0",
                        "providers": [{"name": "ESA", "roles": ["licensor"]}],
                        "extent": {
                            "spatial": {"bbox": [[-20, -20, 40, 40]]},
                            "temporal": {"interval": [["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]]},
                        },
                        "cube:dimensions": {
                            "bands": {"type": "bands", "values": ["B01", "B02"]},
                            "y": {"type": "spatial", "axis": "y"},
                        },
                        "links": [
                            {
                                "rel": "license",
                                "href": "https://spdx.org/licenses/Apache-1.0.html",
                            },
                        ],
                    }
                ]
            },
        )
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
                    "temporal": {
                        "interval": [
                            ["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"],
                            ["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"],
                        ]
                    },
                },
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": ["B01", "B02"]},
                    "x": {"type": "spatial", "extent": [3.0, 4.0], "axis": "x"},
                    "y": {"type": "spatial", "axis": "y"},
                },
                "license": "various",
                "providers": [
                    {"name": "ESA", "roles": ["producer"]},
                    {"name": "ESA", "roles": ["licensor"]},
                ],
                "summaries": {
                    "federation:backends": ["b1", "b2"],
                    "provider:backend": ["b1", "b2"],
                },
                "links": [
                    {"rel": "license", "href": "https://spdx.org/licenses/MIT.html"},
                    {
                        "rel": "license",
                        "href": "https://spdx.org/licenses/Apache-1.0.html",
                    },
                ],
                "type": "Collection",
            },
        ]

    def test_get_best_backend_for_collections_basic(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(
            backend1 + "/collections",
            json={"collections": [{"id": "S3"}, {"id": "S4"}, {"id": "S666"}]},
        )
        requests_mock.get(
            backend2 + "/collections",
            json={"collections": [{"id": "S4"}, {"id": "S5"}, {"id": "S777"}]},
        )
        with pytest.raises(OpenEOApiException, match="Empty collection set given"):
            catalog.get_backend_candidates_for_collections([])
        assert catalog.get_backend_candidates_for_collections(["S3"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4"]) == ["b1", "b2"]
        assert catalog.get_backend_candidates_for_collections(["S5"]) == ["b2"]
        assert catalog.get_backend_candidates_for_collections(["S3", "S4"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4", "S5"]) == ["b2"]

        with pytest.raises(
            OpenEOApiException,
            match=r"Requested collections are not available on a single backend, but spread across separate ones: 'S3' only on \['b1'\], 'S5' only on \['b2'\]\.",
        ):
            catalog.get_backend_candidates_for_collections(["S3", "S4", "S5"])

    def test_get_collection_metadata_basic(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend2 + "/collections/S3", json={"id": "S3", "title": "b2's S3"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "title": "b1's S2",
            "summaries": {"federation:backends": ["b1"]},
        }
        metadata = catalog.get_collection_metadata("S3")
        assert metadata == {
            "id": "S3",
            "title": "b2's S3",
            "summaries": {"federation:backends": ["b2"]},
        }

        with pytest.raises(CollectionNotFoundException):
            catalog.get_collection_metadata("S5")

    def test_get_collection_metadata_merging(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "title": "b1's S2",
                "stac_version": "0.9.0",
                "stac_extensions": ["datacube", "sar"],
                "crs": [
                    "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                    "http://www.opengis.net/def/crs/EPSG/0/2154",
                ],
                "keywords": ["S2", "Sentinel Hub"],
                "version": "1.1.0",
                "license": "special",
                "links": [
                    {"rel": "license", "href": "https://special.license.org"},
                ],
                "providers": [{"name": "provider1"}],
                "sci:citation": "Modified Copernicus Sentinel data [Year]/Sentinel Hub",
                "datasource_type": "1sentinel-2-grd",
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "title": "b2's S2",
                "stac_version": "1.0.0",
                "stac_extensions": [
                    "https://stac-extensions.github.io/datacube/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/raster/v1.0.0/schema.json",
                ],
                "keywords": ["xcube", "SAR"],
                "version": "0.8.0",
                "deprecated": False,
                "license": "proprietary",
                "links": [
                    {"rel": "license", "href": "https://propietary.license"},
                ],
                "providers": [{"name": "provider2"}],
                "datasource_type": "2sentinel-2-grd",
                "sci:citation": "Second citation list.",
            },
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "description": "S2",
            "title": "b1's S2",
            "stac_version": "1.0.0",
            "stac_extensions": [
                "datacube",
                "sar",
                "https://stac-extensions.github.io/datacube/v1.0.0/schema.json",
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
                "https://stac-extensions.github.io/raster/v1.0.0/schema.json",
            ],
            "crs": [
                "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                "http://www.opengis.net/def/crs/EPSG/0/2154",
            ],
            "keywords": ["S2", "Sentinel Hub", "xcube", "SAR"],
            "version": "1.1.0",
            "deprecated": False,
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "license": "various",
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
            "links": [
                {"rel": "license", "href": "https://special.license.org"},
                {"rel": "license", "href": "https://propietary.license"},
            ],
            "providers": [{"name": "provider1"}, {"name": "provider2"}],
            "type": "Collection",
            "sci:citation": "Modified Copernicus Sentinel data [Year]/Sentinel Hub",
        }

    def test_get_collection_metadata_merging_summaries(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "summaries": {
                    "constellation": ["sentinel-1"],
                    "instruments": ["c-sar"],
                    "platform": ["sentinel-1a", "sentinel-1b"],
                    "sar:center_frequency": [5.405],
                    "sar:frequency_band": ["C"],
                    "sar:instrument_mode": ["SM", "IW", "EW", "WV"],
                    "sar:polarizations": [
                        "SH",
                        "SV",
                        "DH",
                        "DV",
                        "HH",
                        "HV",
                        "VV",
                        "VH",
                    ],
                    "sar:product_type": ["GRD"],
                    "sar:resolution": [10, 25, 40],
                    "sat:orbit_state": ["ascending", "descending"],
                },
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "summaries": {
                    "constellation": ["sentinel-1"],
                    "instruments": ["c-sar"],
                    "platform": ["sentinel-1"],
                    "sar:center_frequency": [5.405],
                    "sar:frequency_band": ["C"],
                    "sar:instrument_mode": ["IW"],
                    "sar:looks_azimuth": [1],
                    "sar:looks_equivalent_number": [4.4],
                    "sar:looks_range": [5],
                    "sar:pixel_spacing_azimuth": [10],
                    "sar:pixel_spacing_range": [10],
                    "sar:polarizations": ["HH", "VV", "VV+VH", "HH+HV"],
                    "sar:product_type": ["GRD"],
                    "sar:resolution_azimuth": [22],
                    "sar:resolution_range": [20],
                },
            },
        )
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "description": "S2",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "id": "S2",
            "license": "proprietary",
            "stac_version": "0.9.0",
            "summaries": {
                "constellation": ["sentinel-1"],
                "instruments": ["c-sar"],
                "platform": ["sentinel-1a", "sentinel-1b", "sentinel-1"],
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
                "sar:center_frequency": [5.405],
                "sar:frequency_band": ["C"],
                "sar:instrument_mode": ["SM", "IW", "EW", "WV"],
                "sar:looks_azimuth": [1],
                "sar:looks_equivalent_number": [4.4],
                "sar:looks_range": [5],
                "sar:pixel_spacing_azimuth": [10],
                "sar:pixel_spacing_range": [10],
                "sar:polarizations": [
                    "SH",
                    "SV",
                    "DH",
                    "DV",
                    "HH",
                    "HV",
                    "VV",
                    "VH",
                    "VV+VH",
                    "HH+HV",
                ],
                "sar:product_type": ["GRD"],
                "sar:resolution": [10, 25, 40],
                "sar:resolution_azimuth": [22],
                "sar:resolution_range": [20],
                "sat:orbit_state": ["ascending", "descending"],
            },
            "title": "S2",
            "type": "Collection",
        }

    def test_get_collection_metadata_merging_extent(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90], [-10, -90, 120, 90]]},
                    "temporal": {"interval": [["2014-10-03T04:14:15Z", None]]},
                },
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {
                        "interval": [
                            ["2014-10-03T04:14:15Z", None],
                            [
                                None,
                                None,
                            ],
                        ]
                    },
                },
            },
        )
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "stac_version": "0.9.0",
            "title": "S2",
            "description": "S2",
            "type": "Collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90], [-10, -90, 120, 90]]},
                "temporal": {"interval": [["2014-10-03T04:14:15Z", None]]},
            },
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
        }

    def test_get_collection_metadata_merging_links(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "links": [
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "root",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "parent",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S2",
                        "rel": "self",
                    },
                    {"href": "http://some.license", "rel": "license"},
                ],
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "links": [
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S2",
                        "rel": "self",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "parent",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "root",
                    },
                ],
            },
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "stac_version": "0.9.0",
            "title": "S2",
            "description": "S2",
            "type": "Collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {"href": "http://some.license", "rel": "license"},
            ],
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
        }

    def test_get_collection_metadata_merging_removes_duplicate_links(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "links": [
                    {
                        "href": f"{backend1}/collections",
                        "rel": "root",
                    },
                    {
                        "href": f"{backend1}/collections",
                        "rel": "parent",
                    },
                    {
                        "href": f"{backend1}/collections/S2",
                        "rel": "self",
                    },
                    {"href": "http://some.license", "rel": "license"},
                    {"href": "http://some.about.page", "rel": "about"},
                ],
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "links": [
                    {
                        "href": f"{backend2}/collections/S2",
                        "rel": "self",
                    },
                    {
                        "href": f"{backend2}/collections",
                        "rel": "parent",
                    },
                    {
                        "href": f"{backend2}/collections",
                        "rel": "root",
                    },
                    {"href": "http://some.license", "rel": "license"},
                    {"href": "http://some.about.page", "rel": "about"},
                ],
            },
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "stac_version": "0.9.0",
            "title": "S2",
            "description": "S2",
            "type": "Collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {"href": "http://some.license", "rel": "license"},
                {"href": "http://some.about.page", "rel": "about"},
            ],
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
        }

    def test_get_collection_metadata_merging_cubedimensions(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b1_bands = ["VV", "VH", "HV", "HH"]
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": b1_bands},
                    "t": {
                        "extent": ["2013-10-03T04:14:15Z", "2020-04-03T00:00:00Z"],
                        "step": 1,
                        "type": "temporal",
                    },
                    "x": {
                        "axis": "x",
                        "extent": [-20, 130],
                        "reference_system": {"name": "PROJJSON object."},
                        "step": 10,
                        "type": "spatial",
                    },
                    "y": {
                        "axis": "y",
                        "extent": [-10, 20],
                        "reference_system": {"name": "PROJJSON object."},
                        "step": 10,
                        "type": "spatial",
                    },
                },
                "summaries": {"eo:bands": [{"name": b} for b in b1_bands]},
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        b2_bands = ["VV", "VH", "HH", "HH+HV", "VV+VH", "HV"]
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "cube:dimensions": {
                    "bands": {
                        "type": "bands",
                        "values": b2_bands,
                    },
                    "t": {
                        "extent": ["2013-04-03T00:00:00Z", "2019-04-03T00:00:00Z"],
                        "step": 1,
                        "type": "temporal",
                    },
                    "x": {
                        "axis": "x",
                        "extent": [-40, 120],
                        "type": "spatial",
                        "reference_system": {"name": "PROJJSON object."},
                    },
                    "y": {
                        "axis": "y",
                        "extent": [0, 45],
                        "type": "spatial",
                        "reference_system": {"name": "PROJJSON object."},
                    },
                },
                "summaries": {"eo:bands": [{"name": b} for b in b2_bands]},
            },
        )

        metadata = catalog.get_collection_metadata("S2")
        expected_bands = ["VV", "VH"]
        assert metadata == {
            "id": "S2",
            "stac_version": "0.9.0",
            "title": "S2",
            "description": "S2",
            "type": "Collection",
            "license": "proprietary",
            "cube:dimensions": {
                "bands": {
                    "type": "bands",
                    "values": expected_bands,
                },
                "t": {
                    "extent": ["2013-04-03T00:00:00Z", "2020-04-03T00:00:00Z"],
                    "step": 1,
                    "type": "temporal",
                },
                "x": {
                    "axis": "x",
                    "extent": [-40, 130],
                    "reference_system": {"name": "PROJJSON object."},
                    "step": 10,
                    "type": "spatial",
                },
                "y": {
                    "axis": "y",
                    "extent": [-10, 45],
                    "reference_system": {"name": "PROJJSON object."},
                    "step": 10,
                    "type": "spatial",
                },
            },
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
                "eo:bands": [{"name": b} for b in expected_bands],
            },
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

    @pytest.mark.parametrize(
        ["b1_bands", "b2_bands", "expected_bands"],
        [
            (["VV", "HH"], ["VV", "HH"], ["VV", "HH"]),
            (["VV", "HH"], ["VV", "HH", "HH+VH"], ["VV", "HH"]),
            (["VV", "HH"], ["VV", "VH"], ["VV"]),
            (
                ["VV", "VH", "HV", "HH"],
                ["HH", "VV", "HH+HV", "VV+VH"],
                ["VV", "VH", "HV", "HH"],
            ),
        ],
    )
    def test_get_collection_metadata_merging_bands_prefix(
        self,
        catalog,
        backend1,
        backend2,
        requests_mock,
        b1_bands,
        b2_bands,
        expected_bands,
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend1 + "/collections/S2",
            json={
                "id": "S2",
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": b1_bands},
                },
                "summaries": {"eo:bands": [{"name": b} for b in b1_bands]},
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(
            backend2 + "/collections/S2",
            json={
                "id": "S2",
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": b2_bands},
                },
                "summaries": {"eo:bands": [{"name": b} for b in b2_bands]},
            },
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "stac_version": "0.9.0",
            "title": "S2",
            "description": "S2",
            "type": "Collection",
            "license": "proprietary",
            "cube:dimensions": {
                "bands": {
                    "type": "bands",
                    "values": expected_bands,
                },
            },
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
                "eo:bands": [{"name": b} for b in expected_bands],
            },
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

    def test_get_collection_metadata_merging_with_error(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", status_code=500)
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "title": "b2's S2",
            "summaries": {"federation:backends": ["b2"]},
        }
        # TODO: test that caching of result is different from merging without error? (#2)

    # TODO tests for caching of collection metadata

    def test_generate_backend_constraint_callables(self):
        callables = AggregatorCollectionCatalog.generate_backend_constraint_callables(
            [
                {
                    "eq": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "b1"},
                        "result": True,
                    }
                },
                {
                    "eq": {
                        "process_id": "neq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "b2"},
                        "result": True,
                    }
                },
            ]
        )
        equals_b1, differs_from_b2 = callables
        assert equals_b1("b1") is True
        assert equals_b1("b2") is False
        assert equals_b1("b3") is False
        assert differs_from_b2("b1") is True
        assert differs_from_b2("b2") is False
        assert differs_from_b2("b3") is True

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {tuple}),
            ({"memoizer": {"type": "jsondict", "config": {"default_ttl": 66}}}, {bytes}),
        ],
    )
    def test_get_all_metadata_caching(
        self, multi_backend_connection, backend1, backend2, requests_mock, overrides, expected_cache_types
    ):
        b1am = requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b2am = requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        with config_overrides(**overrides):
            catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)

        metadata = catalog.get_all_metadata()
        assert metadata == [DictSubSet({"id": "S2"})]
        assert (b1am.call_count, b2am.call_count) == (1, 1)

        with clock_mock(offset=10):
            metadata = catalog.get_all_metadata()
            assert metadata == [DictSubSet({"id": "S2"})]
            assert (b1am.call_count, b2am.call_count) == (1, 1)

        with clock_mock(offset=100):
            metadata = catalog.get_all_metadata()
            assert metadata == [DictSubSet({"id": "S2"})]
            assert (b1am.call_count, b2am.call_count) == (2, 2)

        assert isinstance(catalog._memoizer, DictMemoizer)
        cache_dump = catalog._memoizer.dump(values_only=True)
        assert set(type(v) for v in cache_dump) == expected_cache_types

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {tuple, dict}),
            ({"memoizer": {"type": "jsondict"}}, {bytes}),
        ],
    )
    def test_get_collection_metadata_caching(
        self, multi_backend_connection, backend1, backend2, requests_mock, overrides, expected_cache_types
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b1s2 = requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        b2s2 = requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        with config_overrides(**overrides):
            catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == DictSubSet({"id": "S2", "title": "b1's S2"})
        assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=10):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({"id": "S2", "title": "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=100):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({"id": "S2", "title": "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (2, 2)

        assert isinstance(catalog._memoizer, DictMemoizer)
        cache_dump = catalog._memoizer.dump(values_only=True)
        assert set(type(v) for v in cache_dump) == expected_cache_types


class TestJobIdMapping:
    def test_get_aggregator_job_id(self):
        assert (
            JobIdMapping.get_aggregator_job_id(backend_job_id="j0bId-f00b6r", backend_id="vito") == "vito-j0bId-f00b6r"
        )

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


from openeo_aggregator.backend import ServiceIdMapping


class TestServiceIdMapping:
    def test_get_aggregator_job_id(self):
        assert (
            ServiceIdMapping.get_aggregator_service_id(backend_service_id="service-x17-abc", backend_id="vito")
            == "vito-service-x17-abc"
        )

    def test_parse_aggregator_job_id(self, multi_backend_connection):
        assert ServiceIdMapping.parse_aggregator_service_id(
            backends=multi_backend_connection, aggregator_service_id="b1-serv021b"
        ) == ("serv021b", "b1")
        assert ServiceIdMapping.parse_aggregator_service_id(
            backends=multi_backend_connection, aggregator_service_id="b2-someservice-321-ab14jh"
        ) == ("someservice-321-ab14jh", "b2")

    def test_parse_aggregator_job_id_fail(self, multi_backend_connection):
        with pytest.raises(ServiceNotFoundException):
            ServiceIdMapping.parse_aggregator_service_id(
                backends=multi_backend_connection, aggregator_service_id="b3-b6tch-j0b-o123423"
            )


class TestAggregatorProcessing:
    def test_get_process_registry_basic(
        self,
        catalog,
        multi_backend_connection,
        backend1,
        backend2,
        requests_mock,
    ):
        requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {
                        "id": "add",
                        "parameters": [
                            {"name": "x", "schema": {}},
                            {"name": "y", "schema": {}},
                        ],
                    },
                    {"id": "mean", "parameters": [{"name": "data", "schema": {}}]},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {
                        "id": "multiply",
                        "parameters": [
                            {"name": "x", "schema": {}},
                            {"name": "y", "schema": {}},
                        ],
                    },
                    {"id": "mean", "parameters": [{"name": "data", "schema": {}}]},
                ]
            },
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {
                "id": "add",
                "description": "add",
                "parameters": [
                    {"name": "x", "schema": {}, "description": "x"},
                    {"name": "y", "schema": {}, "description": "y"},
                ],
                "returns": {"schema": {}},
                "federation:backends": ["b1"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
            {
                "id": "mean",
                "description": "mean",
                "parameters": [{"name": "data", "schema": {}, "description": "data"}],
                "returns": {"schema": {}},
                "federation:backends": ["b1", "b2"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
            {
                "id": "multiply",
                "description": "multiply",
                "parameters": [
                    {"name": "x", "schema": {}, "description": "x"},
                    {"name": "y", "schema": {}, "description": "y"},
                ],
                "returns": {"schema": {}},
                "federation:backends": ["b2"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
        ]

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {dict}),
            ({"memoizer": {"type": "jsondict"}}, {bytes}),
        ],
    )
    def test_get_process_registry_caching(
        self, multi_backend_connection, backend1, backend2, requests_mock, overrides, expected_cache_types
    ):
        b1p = requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
                ]
            },
        )
        b2p = requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
                ]
            },
        )

        with config_overrides(**overrides):
            catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
            processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)

        assert (b1p.call_count, b2p.call_count) == (0, 0)

        _ = processing.get_process_registry(api_version="1.0.0")
        assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=10):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=1000):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (2, 2)

        assert isinstance(processing._memoizer, DictMemoizer)
        cache_dump = processing._memoizer.dump(values_only=True)
        assert set(type(v) for v in cache_dump) == expected_cache_types

    def test_get_process_registry_parameter_differences(
        self,
        catalog,
        multi_backend_connection,
        backend1,
        backend2,
        requests_mock,
    ):
        requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {
                        "id": "add",
                        "parameters": [
                            {"name": "x", "schema": {}},
                            {"name": "y", "schema": {}},
                        ],
                    },
                    {"id": "mean", "parameters": [{"name": "array", "schema": {}}]},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {
                        "id": "multiply",
                        "parameters": [
                            {"name": "x", "schema": {}},
                            {"name": "y", "schema": {}},
                        ],
                    },
                    {"id": "mean", "parameters": [{"name": "values", "schema": {}}]},
                ]
            },
        )
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {
                "id": "add",
                "description": "add",
                "parameters": [
                    {"name": "x", "schema": {}, "description": "x"},
                    {"name": "y", "schema": {}, "description": "y"},
                ],
                "returns": {"schema": {}},
                "federation:backends": ["b1"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
            {
                "id": "mean",
                "description": "mean",
                "parameters": [{"name": "array", "schema": {}, "description": "array"}],
                "returns": {"schema": {}},
                "federation:backends": ["b1", "b2"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
            {
                "id": "multiply",
                "description": "multiply",
                "parameters": [
                    {"name": "x", "schema": {}, "description": "x"},
                    {"name": "y", "schema": {}, "description": "y"},
                ],
                "returns": {"schema": {}},
                "federation:backends": ["b2"],
                "deprecated": False,
                "experimental": False,
                "examples": [],
                "links": [],
            },
        ]

    def test_get_process_registry_allow_list_deny_experimental(
        self,
        catalog,
        multi_backend_connection,
        backend1,
        backend2,
        requests_mock,
    ):
        requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {"id": "mean", "parameters": []},
                    {"id": "avg", "parameters": [], "experimental": True},
                    {"id": "average", "parameters": []},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {"id": "mean", "parameters": []},
                    {"id": "avg", "parameters": [], "experimental": True},
                    {"id": "average", "parameters": [], "experimental": True},
                ]
            },
        )

        process_allowed: ProcessAllowed = lambda process_id, backend_id, experimental: not experimental
        with config_overrides(process_allowed=process_allowed):
            processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
            registry = processing.get_process_registry(api_version="1.0.0")

        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            IsPartialDict(**{"id": "average", "experimental": False, "federation:backends": ["b1"]}),
            IsPartialDict(**{"id": "mean", "experimental": False, "federation:backends": ["b1", "b2"]}),
        ]

    def test_get_process_registry_allow_list_deny_by_backend(
        self,
        catalog,
        multi_backend_connection,
        backend1,
        backend2,
        requests_mock,
    ):
        requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {"id": "mean", "parameters": []},
                    {"id": "avg", "parameters": []},
                    {"id": "average", "parameters": []},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {"id": "mean", "parameters": []},
                    {"id": "avg", "parameters": []},
                    {"id": "_experimental_foo", "parameters": []},
                ]
            },
        )

        def process_allowed(process_id: str, backend_id: str, experimental) -> bool:
            return not any(
                [
                    (backend_id == "b1" and process_id.startswith("av")),
                    (backend_id == "b2" and process_id.startswith("_")),
                ]
            )

        with config_overrides(process_allowed=process_allowed):
            processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog)
            registry = processing.get_process_registry(api_version="1.0.0")

        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            IsPartialDict(**{"id": "avg", "federation:backends": ["b2"]}),
            IsPartialDict(**{"id": "mean", "federation:backends": ["b1", "b2"]}),
        ]
