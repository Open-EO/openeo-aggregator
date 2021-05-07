import pytest

from openeo import Connection
from openeo_aggregator.backend import AggregatorCollectionCatalog, MultiBackendConnection, BackendConnection

import types


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


class TestAggregatorCollectionCatalog:

    def test_get_all_metadata(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get("https://oeo2.test/collections", json={"collections": [{"id": "S3"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [{"id": "S2"}, {"id": "S3"}, ]

    def test_get_collection_metadata(self, multi_backend_connection, requests_mock):
        requests_mock.get("https://oeo1.test/collections/S2", status_code=400)
        requests_mock.get("https://oeo2.test/collections/S2", json={"id": "S2", "title": "oeo2's S2"})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {"id": "S2", "title": "oeo2's S2"}

    # TODO tests for caching of collection metadata
