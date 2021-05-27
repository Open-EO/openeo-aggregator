import types

import pytest

from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo_aggregator.backend import AggregatorCollectionCatalog, MultiBackendConnection, BackendConnection, \
    AggregatorProcessing, AggregatorBackendImplementation


@pytest.fixture
def multi_backend_connection(requests_mock) -> MultiBackendConnection:
    requests_mock.get("https://oeo1.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo2.test/", json={"api_version": "1.0.0"})
    return MultiBackendConnection({
        "oeo1": "https://oeo1.test/",
        "oeo2": "https://oeo2.test/",
    })


class TestMultiBackendConnection:

    # TODO test version discovery in constructor

    def test_iter(self, multi_backend_connection):
        count = 0
        for x in multi_backend_connection:
            assert isinstance(x, BackendConnection)
            assert isinstance(x.connection, Connection)
            count += 1
        assert count == 2

    def test_map(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/foo", json={"bar": 1})
        requests_mock.get("https://oeo2.test/foo", json={"meh": 2})
        res = multi_backend_connection.map(lambda connection: connection.get("foo").json())
        assert isinstance(res, types.GeneratorType)
        assert list(res) == [("oeo1", {"bar": 1}), ("oeo2", {"meh": 2})]

    def test_api_version(self, multi_backend_connection):
        assert multi_backend_connection.api_version == ComparableVersion("1.0.0")


class TestAggregatorBackendImplementation:

    def test_oidc_providers(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/credentials/oidc", json={"providers": [
            {"id": "x", "issuer": "https://x.test", "title": "X"},
            {"id": "y", "issuer": "https://y.test", "title": "YY"},
        ]})
        requests_mock.get("https://oeo2.test/credentials/oidc", json={"providers": [
            {"id": "y", "issuer": "https://y.test", "title": "YY"},
            {"id": "z", "issuer": "https://z.test", "title": "ZZZ"},
        ]})
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection)
        providers = implementation.oidc_providers()
        assert len(providers) == 1
        provider = providers[0]
        expected = {"id": "y", "issuer": "https://y.test", "title": "YY", "scopes": ["openid"]}
        assert provider.prepare_for_json() == expected


class TestAggregatorCollectionCatalog:

    def test_get_all_metadata(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get("https://oeo2.test/collections", json={"collections": [{"id": "S3"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [{"id": "S2"}, {"id": "S3"}, ]

    def test_get_all_metadata_duplicate(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get("https://oeo2.test/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [{"id": "S3"}, {"id": "S5"}, ]

    def test_get_collection_metadata(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/collections/S2", status_code=400)
        requests_mock.get("https://oeo2.test/collections/S2", json={"id": "S2", "title": "oeo2's S2"})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {"id": "S2", "title": "oeo2's S2"}

    # TODO tests for caching of collection metadata


class TestAggregatorProcessing:

    def test_get_process_registry(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        requests_mock.get("https://oeo2.test/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        processing = AggregatorProcessing(backends=multi_backend_connection)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert registry.get_specs() == [
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]
