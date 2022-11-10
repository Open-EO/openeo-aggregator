from sys import implementation
from datetime import datetime

import pytest

from openeo_aggregator.backend import AggregatorCollectionCatalog, AggregatorProcessing, \
    AggregatorBackendImplementation, _InternalCollectionMetadata, JobIdMapping
from openeo_aggregator.caching import DictMemoizer
from openeo_aggregator.testing import clock_mock
from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import OpenEOApiException, CollectionNotFoundException, JobNotFoundException, \
    ServiceNotFoundException
from openeo_driver.testing import DictSubSet
from openeo_driver.users.oidc import OidcProvider
from openeo_driver.users.auth import HttpAuthHandler
from openeo_driver.errors import ProcessGraphMissingException, ProcessGraphInvalidException, ServiceUnsupportedException
from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError, OpenEoRestError
from .conftest import DEFAULT_MEMOIZER_CONFIG, set_backend_to_api_version


TEST_USER = "Mr.Test"
TEST_USER_BEARER_TOKEN = "basic//" + HttpAuthHandler.build_basic_access_token(user_id=TEST_USER)
TEST_USER_AUTH_HEADER = {
    "Authorization": "Bearer " + TEST_USER_BEARER_TOKEN
}

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

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_file_formats_caching(
            self,
            multi_backend_connection, config, backend1, backend2, requests_mock, memoizer_config,
    ):
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
        with clock_mock(offset=100):
            _ = implementation.file_formats()
        assert mock1.call_count == 2
        assert mock2.call_count == 2

        assert isinstance(implementation._memoizer, DictMemoizer)
        cache_dump = implementation._memoizer.dump(values_only=True)
        assert len(cache_dump) == 1
        expected_type = {"dict": dict, "jsondict": bytes}[memoizer_config["type"]]
        assert all(isinstance(x, expected_type) for x in cache_dump)

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


class TestAggregatorSecondaryServices:

    def test_service_types_simple(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        """Given 2 backends and only 1 backend has a single service type, then the aggregator
            returns that 1 service type's metadata.
        """
        single_service_type = {
            "WMTS": {
                "configuration": {
                    "colormap": {
                        "default": "YlGn",
                        "description":
                        "The colormap to apply to single band layers",
                        "type": "string"
                    },
                    "version": {
                        "default": "1.0.0",
                        "description": "The WMTS version to use.",
                        "enum": ["1.0.0"],
                        "type": "string"
                    }
                },
                "links": [],
                "process_parameters": [],
                "title": "Web Map Tile Service"
            }
        }
        requests_mock.get(backend1 + "/service_types", json=single_service_type)
        requests_mock.get(backend2 + "/service_types", json=single_service_type)
        abe_implementation = AggregatorBackendImplementation(
            backends=multi_backend_connection, config=config
        )
        service_types = abe_implementation.service_types()
        assert service_types == single_service_type

    def test_service_types_merging(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        """Given 2 backends with each 1 service type, then the aggregator lists both service types."""
        service_type_1 = {
            "WMTS": {
                "configuration": {
                    "colormap": {
                        "default": "YlGn",
                        "description":
                        "The colormap to apply to single band layers",
                        "type": "string"
                    },
                    "version": {
                        "default": "1.0.0",
                        "description": "The WMTS version to use.",
                        "enum": ["1.0.0"],
                        "type": "string"
                    }
                },
                "links": [],
                "process_parameters": [],
                "title": "Web Map Tile Service"
            }
        }
        service_type_2 = {
            "WMS": {
                "title": "OGC Web Map Service",
                "configuration": {},
                "process_parameters": [],
                "links": []
            }
        }
        requests_mock.get(backend1 + "/service_types", json=service_type_1)
        requests_mock.get(backend2 + "/service_types", json=service_type_2)
        abe_implementation = AggregatorBackendImplementation(
            backends=multi_backend_connection, config=config
        )

        actual_service_types = abe_implementation.service_types()

        expected_service_types = dict(service_type_1)
        expected_service_types.update(service_type_2)
        assert actual_service_types == expected_service_types

    @pytest.fixture
    def service_metadata_wmts_foo(self):
        return ServiceMetadata(
            id="wmts-foo",
            process={"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}},
            url='https://oeo.net/wmts/foo',
            type="WMTS",
            enabled=True,
            configuration={"version": "0.5.8"},
            attributes={},
            title="Test WMTS service",
            created=datetime(2020, 4, 9, 15, 5, 8)
        )

    @pytest.fixture
    def service_metadata_wms_bar(self):
        return ServiceMetadata(
            id="wms-bar",
            process={"process_graph": {"bar": {"process_id": "bar", "arguments": {}}}},
            url='https://oeo.net/wms/bar',
            type="WMS",
            enabled=True,
            configuration={"version": "0.5.8"},
            attributes={},
            title="Test WMS service",
            created=datetime(2022, 2, 1, 13, 30, 3)
        )

    def test_list_services_simple(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock,
        service_metadata_wmts_foo
    ):
        """Given 2 backends but only 1 backend has a single service, then the aggregator
            returns that 1 service's metadata.
        """
        services1 = {"services": [service_metadata_wmts_foo.prepare_for_json()], "links": []}
        services2 = {}
        requests_mock.get(backend1 + "/services", json=services1)
        requests_mock.get(backend2 + "/services", json=services2)
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            actual_services = abe_implementation.list_services(user_id=TEST_USER)

            # Construct expected result. We have get just data from the service in services1
            # (there is only one) for conversion to a ServiceMetadata.
            the_service = services1["services"][0]
            expected_services = [
                ServiceMetadata.from_dict(the_service)
            ]
            assert actual_services == expected_services

    def test_list_services_merged(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock,
        service_metadata_wmts_foo, service_metadata_wms_bar
    ):
        """Given 2 backends with each 1 service, then the aggregator lists both services."""

        services1 = {"services": [service_metadata_wmts_foo.prepare_for_json()], "links": []}
        services2 = {"services": [service_metadata_wms_bar.prepare_for_json()], "links": []}
        requests_mock.get(backend1 + "/services", json=services1)
        requests_mock.get(backend2 + "/services", json=services2)
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            actual_services = abe_implementation.list_services(user_id=TEST_USER)

            expected_services = [service_metadata_wmts_foo, service_metadata_wms_bar]
            assert sorted(actual_services) == sorted(expected_services)

    def test_list_services_merged_multiple(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock,
        service_metadata_wmts_foo, service_metadata_wms_bar
    ):
        """Given multiple services across 2 backends, the aggregator lists all service types from all backends."""
        services1 = {
            "services": [{
                "id": "wms-nvdi",
                "title": "NDVI based on Sentinel 2",
                "description": "Deriving minimum NDVI measurements over pixel time series of Sentinel 2",
                "url": "https://example.openeo.org/wms/wms-nvdi",
                "type": "wms",
                "enabled": True,
                "process": {
                    "id": "ndvi",
                    "summary": "string",
                    "description": "string",
                    "links": [{
                        "rel": "related",
                        "href": "https://example.openeo.org",
                        "type": "text/html",
                        "title": "openEO"
                    }],
                    "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
                },
                "configuration": {
                    "version": "1.3.0"
                },
                "attributes": {
                    "layers": ["ndvi", "evi"]
                },
                "created": "2017-01-01T09:32:12Z",
            }],
            "links": [{
                "rel": "related",
                "href": "https://example.openeo.org",
                "type": "text/html",
                "title": "openEO"
            }]
        }
        services2 = {"services": [
                service_metadata_wmts_foo.prepare_for_json(),
                service_metadata_wms_bar.prepare_for_json()
            ],
            "links": []
        }
        requests_mock.get(backend1 + "/services", json=services1)
        requests_mock.get(backend2 + "/services", json=services2)
        abe_implementation = AggregatorBackendImplementation(
            backends=multi_backend_connection, config=config
        )
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            actual_services = abe_implementation.list_services(user_id=TEST_USER)

            # Construct expected result. We have get just data from the service in
            # services1 (there is only one) for conversion to a ServiceMetadata.
            # TODO: do we need to take care of the links part in the JSON as well?
            service1 = services1["services"][0]
            service1_md = ServiceMetadata.from_dict(service1)
            expected_services = [
                service1_md, service_metadata_wmts_foo, service_metadata_wms_bar
            ]

            assert sorted(actual_services) == sorted(expected_services)

    def test_service_info_succeeds(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock,
        service_metadata_wmts_foo, service_metadata_wms_bar
    ):
        """When it gets a correct service ID, it returns the expected ServiceMetadata."""
        requests_mock.get(backend1 + "/services/wmts-foo", json=service_metadata_wmts_foo.prepare_for_json())
        requests_mock.get(backend2 + "/services/wms-bar", json=service_metadata_wms_bar.prepare_for_json())
        abe_implementation = AggregatorBackendImplementation(
            backends=multi_backend_connection, config=config
        )
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        # Check the expected metadata on *both* of the services.
        with flask_app.test_request_context(headers=headers):
            actual_service1 = abe_implementation.service_info(user_id=TEST_USER, service_id="wmts-foo")
            assert actual_service1 == service_metadata_wmts_foo

        with flask_app.test_request_context(headers=headers):
            actual_service2 = abe_implementation.service_info(user_id=TEST_USER, service_id="wms-bar")
            assert actual_service2 == service_metadata_wms_bar

    def test_service_info_wrong_id(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock,
        service_metadata_wmts_foo, service_metadata_wms_bar
    ):
        """When it gets a non-existent service ID, it raises a ServiceNotFoundException."""

        requests_mock.get(backend1 + "/services/wmts-foo", json=service_metadata_wmts_foo.prepare_for_json())
        requests_mock.get(backend2 + "/services/wms-bar", json=service_metadata_wms_bar.prepare_for_json())
        abe_implementation = AggregatorBackendImplementation(
            backends=multi_backend_connection, config=config
        )
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(ServiceNotFoundException):
                abe_implementation.service_info(user_id=TEST_USER, service_id="doesnotexist")

    def test_create_service_succeeds(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, requests_mock
    ):
        """When it gets a correct params for a new service, it successfully creates it."""

        # Set up responses for creating the service in backend 1
        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        expected_openeo_id = "wmts-foo"
        location_backend_1 = backend1 + "/services/" + expected_openeo_id
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        requests_mock.post(
            backend1 + "/services",
            headers={
                "OpenEO-Identifier": expected_openeo_id,
                "Location": location_backend_1
            },
            status_code=201
        )

        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            actual_openeo_id = abe_implementation.create_service(
                user_id=TEST_USER,
                process_graph=process_graph,
                service_type="WMTS",
                api_version=api_tester.api_version,
                configuration={}
            )
            assert actual_openeo_id == expected_openeo_id

    @pytest.mark.parametrize("exception_class", [OpenEoApiError, OpenEoRestError])
    def test_create_service_backend_raises_openeoapiexception(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock, exception_class
    ):
        """When the backend raises a general exception the aggregator raises an OpenEOApiException."""

        # Set up responses for creating the service in backend 1:
        # This time the backend raises an error, one that will be reported as a OpenEOApiException. 
        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        set_backend_to_api_version(requests_mock, backend2, api_tester.api_version)
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        requests_mock.post(
            backend1 + "/services",
            exc=exception_class("Some server error"),
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(OpenEOApiException):
                abe_implementation.create_service(
                    user_id=TEST_USER,
                    process_graph=process_graph,
                    service_type="WMTS",
                    api_version=api_tester.api_version,
                    configuration={}
                )

    @pytest.mark.parametrize("exception_class",
        [ProcessGraphMissingException, ProcessGraphInvalidException, ServiceUnsupportedException]
    )
    def test_create_service_backend_reraises(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, requests_mock, exception_class
    ):
        """When the backend raises exception types that indicate client error / bad input data,
        the aggregator raises and OpenEOApiException.
        """

        # Set up responses for creating the service in backend 1
        # This time the backend raises an error, one that will simply be re-raised/passed on as it is.
        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        requests_mock.post(
            backend1 + "/services",
            exc=exception_class("Some server error")
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            # These exception types should be re-raised, not become an OpenEOApiException.
            with pytest.raises(exception_class):
                abe_implementation.create_service(
                    user_id=TEST_USER,
                    process_graph=process_graph,
                    service_type="WMTS",
                    api_version=api_tester.api_version,
                    configuration={}
                )

    def test_remove_service_succeeds(
        self, flask_app, api_tester, multi_backend_connection, config, 
        backend1, backend2, requests_mock, service_metadata_wmts_foo
    ):
        """When remove_service is called with an existing service ID, it removes service and returns HTTP 204."""

        # Also test that it can skip backends that don't have the service
        mock_get1 = requests_mock.get(
            backend1 + "/services/wmts-foo",
            status_code=404
        )
        # Delete should succeed in backend2 so service should be present first.
        mock_get2 = requests_mock.get(
            backend2 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=200
        )
        mock_delete = requests_mock.delete(backend2 + "/services/wmts-foo", status_code=204)
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            abe_implementation.remove_service(user_id=TEST_USER, service_id="wmts-foo")

            # Make sure the aggregator asked the backend to remove the service.
            assert mock_delete.called

            # Check the other mocks were called too, just to be sure.
            assert mock_get1.called
            assert mock_get2.called

    def test_remove_service_service_id_not_found(
            self, flask_app, api_tester, multi_backend_connection, config,
            backend1, backend2, requests_mock, service_metadata_wmts_foo
    ):
        """When the service ID does not exist then the aggregator raises an ServiceNotFoundException."""

        # Neither backend has the service available, and the aggregator should detect this.
        mock_get1 = requests_mock.get(
            backend1 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=404
        )
        mock_get2 = requests_mock.get(
            backend2 + "/services/wmts-foo",
            status_code=404
        )
        
        # These requests should not be executed, so check they are not called. 
        mock_delete1 = requests_mock.delete(
            backend2 + "/services/wmts-foo",
            status_code=204
        )
        mock_delete2 = requests_mock.delete(
            backend2 + "/services/wmts-foo",
            status_code=204
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(ServiceNotFoundException):
                abe_implementation.remove_service(user_id=TEST_USER, service_id="wmts-foo")

            assert not mock_delete1.called
            assert not mock_delete2.called 
            # Check the other mocks were called too, just to be sure.
            assert mock_get1.called
            assert mock_get2.called

    def test_remove_service_backend_response_is_an_error_status(
            self, flask_app, api_tester, multi_backend_connection, config, backend1, requests_mock, service_metadata_wmts_foo
    ):
        """When the backend response is an error HTTP 400/500 then the aggregator raises an OpenEoApiError."""

        # Will find it on the first backend, and it should skip the second backend so we don't add it to backend2.
        requests_mock.get(
            backend1 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=200
        )
        requests_mock.delete(backend1 + "/services/wmts-foo", status_code=500)
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(OpenEoApiError) as e:
                abe_implementation.remove_service(user_id=TEST_USER, service_id="wmts-foo")

            # If the backend reports HTTP 400/500, we would expect the same status code from the aggregator.
            # TODO: Statement above is an assumption. Is that really what we expect?
            assert e.value.http_status_code == 500

    # TODO: this test still fails with API version 1.0.0
    def test_update_service_succeeds(
        self, flask_app, api_tester, multi_backend_connection, config, backend1, backend2, requests_mock, service_metadata_wmts_foo
    ):
        """When it receives an existing service ID and a correct payload, it updates the expected service."""

        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        set_backend_to_api_version(requests_mock, backend2, api_tester.api_version)
        
        # Also test that it can skip backends that don't have the service
        mock_get1 = requests_mock.get(
            backend1 + "/services/wmts-foo",
            status_code=404
        )
        # Update should succeed in backend2 so service should be present 
        mock_get2 = requests_mock.get(
            backend2 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=200
        )
        mock_patch = requests_mock.patch(
            backend2 + "/services/wmts-foo",
            status_code=204,
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            abe_implementation.update_service(user_id=TEST_USER, service_id="wmts-foo", process_graph=process_graph_after)

            # # Make sure the aggregator asked the backend to remove the service.
            assert mock_patch.called
            # TODO: I am not too sure this json payload is correct. Check with codebases of other backend drivers. 
            comp_api_version = ComparableVersion(api_tester.api_version)
            if comp_api_version < ComparableVersion((1, 0, 0)):
                expected_process = {"process_graph": process_graph_after}
            else:
                expected_process = {"process": {"process_graph": process_graph_after}}

            assert mock_patch.last_request.json() == expected_process

            # Check the other mocks were called too, just to be sure.
            assert mock_get1.called
            assert mock_get2.called

    def test_update_service_service_id_not_found(
        self, flask_app, api_tester, multi_backend_connection, config,
        backend1, backend2, requests_mock, service_metadata_wmts_foo
    ):
        """When the service ID does not exist then the aggregator raises an ServiceNotFoundException."""

        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        set_backend_to_api_version(requests_mock, backend2, api_tester.api_version)
        mock_get1 = requests_mock.get(
            backend1 + "/services/wmts-foo",
            status_code=404
        )
        mock_get2 = requests_mock.get(
            backend2 + "/services/wmts-foo",
            status_code=404
        )
        
        # These requests should not be executed, so check they are not called.
        mock_patch1 = requests_mock.patch(
            backend1 + "/services/wmts-foo",
            status_code=204,
        )
        mock_patch2 = requests_mock.patch(
            backend2 + "/services/wmts-foo",
            status_code=204,
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(ServiceNotFoundException):
                abe_implementation.update_service(user_id=TEST_USER, service_id="wmts-foo", process_graph=process_graph_after)

            assert not mock_patch1.called
            assert not mock_patch2.called
            # Check the other mocks were called too, just to be sure.
            assert mock_get1.called
            assert mock_get2.called

    def test_update_service_backend_response_is_an_error_status(
        self, flask_app, api_tester, multi_backend_connection, config,
        backend1, backend2, requests_mock, service_metadata_wmts_foo
    ):
        """When the backend response is an error HTTP 400/500 then the aggregator raises an OpenEoApiError."""

        set_backend_to_api_version(requests_mock, backend1, api_tester.api_version)
        set_backend_to_api_version(requests_mock, backend2, api_tester.api_version)
        requests_mock.get(
            backend1 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=200
        )
        mock_patch = requests_mock.patch(
            backend1 + "/services/wmts-foo",
            status_code=500,
        )
        abe_implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        process_graph_after = {"bar": {"process_id": "bar", "arguments": {"arg1": "bar"}}}
        api_tester.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        headers = TEST_USER_AUTH_HEADER

        with flask_app.test_request_context(headers=headers):
            with pytest.raises(OpenEoApiError) as e:
                abe_implementation.update_service(user_id=TEST_USER, service_id="wmts-foo", process_graph=process_graph_after)

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


@pytest.mark.usefixtures("flask_app")  # Automatically enter flask app context for `url_for` to work
class TestAggregatorCollectionCatalog:

    def test_get_all_metadata_simple(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
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
                ],
            },
            {
                "id": "S3",
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
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S3",
                        "rel": "self",
                    },
                ],
            },
        ]

    def test_get_all_metadata_common_collections_minimal(
        self, catalog, backend1, backend2, requests_mock
    ):
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
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S3",
                        "rel": "self",
                    },
                ],
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
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S4",
                        "rel": "self",
                    },
                ],
            },
            {
                "id": "S5",
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
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S5",
                        "rel": "self",
                    },
                ],
            },
        ]

    def test_get_all_metadata_common_collections_merging(
        self, catalog, backend1, backend2, requests_mock
    ):
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
                            "temporal": {
                                "interval": [
                                    ["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"]
                                ]
                            },
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
                            "temporal": {
                                "interval": [
                                    ["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]
                                ]
                            },
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
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "root",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections",
                        "rel": "parent",
                    },
                    {
                        "href": "http://oeoa.test/openeo/1.1.0/collections/S4",
                        "rel": "self",
                    },
                ],
                "type": "Collection",
            },
        ]

    def test_get_best_backend_for_collections_basic(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections",
            json={"collections": [{"id": "S3"}, {"id": "S4"}]},
        )
        requests_mock.get(
            backend2 + "/collections",
            json={"collections": [{"id": "S4"}, {"id": "S5"}]},
        )
        with pytest.raises(OpenEOApiException, match="Empty collection set given"):
            catalog.get_backend_candidates_for_collections([])
        assert catalog.get_backend_candidates_for_collections(["S3"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4"]) == ["b1", "b2"]
        assert catalog.get_backend_candidates_for_collections(["S5"]) == ["b2"]
        assert catalog.get_backend_candidates_for_collections(["S3", "S4"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4", "S5"]) == ["b2"]

        with pytest.raises(
            OpenEOApiException, match="Collections across multiple backends"
        ):
            catalog.get_backend_candidates_for_collections(["S3", "S4", "S5"])

    def test_get_collection_metadata_basic(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
        requests_mock.get(
            backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"}
        )
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S3"}]}
        )
        requests_mock.get(
            backend2 + "/collections/S3", json={"id": "S3", "title": "b2's S3"}
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2', 'title': "b1's S2",
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ]
        }
        metadata = catalog.get_collection_metadata("S3")
        assert metadata == {
            "id": "S3", "title": "b2's S3",
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S3', 'rel': 'self'},
            ]
        }

        with pytest.raises(CollectionNotFoundException):
            catalog.get_collection_metadata("S5")

    def test_get_collection_metadata_merging(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
            "providers": [{"name": "provider1"}, {"name": "provider2"}],
            "type": "Collection",
            "sci:citation": "Modified Copernicus Sentinel data [Year]/Sentinel Hub",
        }

    def test_get_collection_metadata_merging_summaries(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
            "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
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

    def test_get_collection_metadata_merging_extent(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
            "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
        }

    def test_get_collection_metadata_merging_links(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
            "summaries": {
                "federation:backends": ["b1", "b2"],
                "provider:backend": ["b1", "b2"],
            },
        }

    def test_get_collection_metadata_merging_cubedimensions(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
            "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
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
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
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
            "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
        }

    def test_get_collection_metadata_merging_with_error(
        self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
        requests_mock.get(backend1 + "/collections/S2", status_code=500)
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )
        requests_mock.get(
            backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"}
        )

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "title": "b2's S2",
            "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
            ],
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

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_all_metadata_caching(self, catalog, backend1, backend2, requests_mock, memoizer_config):
        b1am = requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b2am = requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

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

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_collection_metadata_caching(self, catalog, backend1, backend2, requests_mock, memoizer_config):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b1s2 = requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        b2s2 = requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
        assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=10):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=100):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (2, 2)


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
    def test_get_process_registry(
        self,
        catalog,
        multi_backend_connection,
        config,
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
        processing = AggregatorProcessing(
            backends=multi_backend_connection, catalog=catalog, config=config
        )
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
            },
            {
                "id": "mean",
                "description": "mean",
                "parameters": [{"name": "data", "schema": {}, "description": "data"}],
                "returns": {"schema": {}},
                "federation:backends": ["b1", "b2"],
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
            },
        ]

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_process_registry_caching(
            self, catalog, multi_backend_connection, config, backend1, backend2, requests_mock,
            memoizer_config
    ):
        b1p = requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]})
        b2p = requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]})
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog, config=config)
        assert (b1p.call_count, b2p.call_count) == (0, 0)

        _ = processing.get_process_registry(api_version="1.0.0")
        assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=10):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=1000):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (2, 2)

    def test_get_process_registry_parameter_differences(
            self, catalog, multi_backend_connection, config, backend1, backend2,
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
        processing = AggregatorProcessing(
            backends=multi_backend_connection, catalog=catalog, config=config
        )
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
            },
            {
                "id": "mean",
                "description": "mean",
                "parameters": [{"name": "array", "schema": {}, "description": "array"}],
                "returns": {"schema": {}},
                "federation:backends": ["b1", "b2"],
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
            },
        ]
