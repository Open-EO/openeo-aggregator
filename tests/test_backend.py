from openeo_aggregator.backend import AggregatorCollectionCatalog, MultiBackendConnection

import pytest


@pytest.fixture
def multi_backend_connection(requests_mock) -> MultiBackendConnection:
    requests_mock.get("https://oeo1.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo2.test/", json={"api_version": "1.0.0"})
    return MultiBackendConnection({
        "oeo1": "https://oeo1.test/",
        "oeo2": "https://oeo2.test/",
    })


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
