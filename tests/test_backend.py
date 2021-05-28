import types

import pytest

from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo_aggregator.backend import AggregatorCollectionCatalog, BackendConnection, \
    AggregatorProcessing, AggregatorBackendImplementation


class TestMultiBackendConnection:

    # TODO test version discovery in constructor

    def test_iter(self, multi_backend_connection):
        count = 0
        for x in multi_backend_connection:
            assert isinstance(x, BackendConnection)
            assert isinstance(x.connection, Connection)
            count += 1
        assert count == 2

    def test_map(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/foo", json={"bar": 1})
        requests_mock.get(backend2 + "/foo", json={"meh": 2})
        res = multi_backend_connection.map(lambda connection: connection.get("foo").json())
        assert isinstance(res, types.GeneratorType)
        assert list(res) == [("b1", {"bar": 1}), ("b2", {"meh": 2})]

    def test_api_version(self, multi_backend_connection):
        assert multi_backend_connection.api_version == ComparableVersion("1.0.0")

    def test_oidc_data_default(self, multi_backend_connection, backend1, backend2):
        res = multi_backend_connection.get_oidc_data()
        assert len(res.provider_list) == 1
        provider = res.provider_list[0]
        expected = {"id": "egi", "issuer": "https://egi.test", "title": "EGI", "scopes": ["openid"]}
        assert provider.prepare_for_json() == expected
        assert res.provider_id_map == {"egi": {"b1": "egi", "b2": "egi"}}

    @pytest.mark.parametrize(["issuer_y1", "issuer_y2"], [
        ("https://y.test", "https://y.test"),
        ("https://y.test", "https://y.test/"),
        ("https://y.test/", "https://y.test/"),
    ])
    def test_oidc_data_intersection(
            self, multi_backend_connection, requests_mock, backend1, backend2, issuer_y1, issuer_y2):
        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "x1", "issuer": "https://x.test", "title": "X1"},
            {"id": "y1", "issuer": issuer_y1, "title": "YY1"},
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "y2", "issuer": issuer_y2, "title": "YY2"},
            {"id": "z2", "issuer": "https://z.test", "title": "ZZZ2"},
        ]})
        res = multi_backend_connection.get_oidc_data()

        assert len(res.provider_list) == 1
        provider = res.provider_list[0]
        expected = {"id": "y1", "issuer": "https://y.test", "title": "YY1", "scopes": ["openid"]}
        assert provider.prepare_for_json() == expected

        assert res.provider_id_map == {"y1": {"b1": "y1", "b2": "y2"}}


class TestAggregatorBackendImplementation:

    def test_oidc_providers(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "x", "issuer": "https://x.test", "title": "X"},
            {"id": "y", "issuer": "https://y.test", "title": "YY"},
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
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

    def test_get_all_metadata(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {"id": "S2", '_aggregator': {'backend': {'id': 'b1', 'url': backend1}}},
            {"id": "S3", '_aggregator': {'backend': {'id': 'b2', 'url': backend2}}},
        ]

    def test_get_all_metadata_duplicate(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {"id": "S3", '_aggregator': {'backend': {'id': 'b1', 'url': backend1}}},
            {"id": "S5", '_aggregator': {'backend': {'id': 'b2', 'url': backend2}}},
        ]

    def test_get_collection_metadata(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections/S2", status_code=400)
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})
        catalog = AggregatorCollectionCatalog(backends=multi_backend_connection)
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {"id": "S2", "title": "b2's S2"}

    # TODO tests for caching of collection metadata


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
        assert registry.get_specs() == [
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]
