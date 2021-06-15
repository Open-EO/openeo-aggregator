import types

import flask
import pytest
import requests

from openeo.capabilities import ComparableVersion
from openeo_aggregator.connection import BackendConnection, MultiBackendConnection
from openeo_driver.backend import OidcProvider


class TestMultiBackendConnection:

    # TODO test version discovery in constructor

    def test_iter(self, multi_backend_connection):
        count = 0
        for x in multi_backend_connection:
            assert isinstance(x, BackendConnection)
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

    def test_get_oidc_providers(self, multi_backend_connection, backend1, backend2):
        providers = multi_backend_connection.get_oidc_providers()
        assert providers == [
            OidcProvider(id="egi", issuer="https://egi.test", title="EGI", scopes=["openid"]),
        ]

    @pytest.mark.parametrize(["issuer_y1", "issuer_y2"], [
        ("https://y.test", "https://y.test"),
        ("https://y.test", "https://y.test/"),
        ("https://y.test/", "https://y.test/"),
    ])
    def test_oidc_providers_issuer_intersection(
            self, multi_backend_connection, requests_mock, backend1, backend2, issuer_y1, issuer_y2
    ):
        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "x1", "issuer": "https://x.test", "title": "X1"},
            {"id": "y1", "issuer": issuer_y1, "title": "YY1"},
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "y2", "issuer": issuer_y2, "title": "YY2"},
            {"id": "z2", "issuer": "https://z.test", "title": "ZZZ2"},
        ]})

        providers = multi_backend_connection.get_oidc_providers()
        assert providers == [
            OidcProvider(id="y1", issuer="https://y.test", title="YY1", scopes=["openid"]),
        ]

    def test_oidc_provider_mapping(self, requests_mock):
        domain1 = "https://b1.test/v1"
        requests_mock.get(domain1 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(domain1 + "/credentials/oidc", json={"providers": [
            {"id": "a1", "issuer": "https://a.test/", "title": "A1"},
            {"id": "x1", "issuer": "https://x.test/", "title": "X1"},
            {"id": "y1", "issuer": "https://y.test/", "title": "Y1"},
        ]})
        domain2 = "https://b2.test/v1"
        requests_mock.get(domain2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(domain2 + "/credentials/oidc", json={"providers": [
            {"id": "b2", "issuer": "https://b.test", "title": "B2"},
            {"id": "x2", "issuer": "https://x.test", "title": "X2"},
            {"id": "y2", "issuer": "https://y.test", "title": "Y2"},
        ]})
        domain3 = "https://b3.test/v1"
        requests_mock.get(domain3 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(domain3 + "/credentials/oidc", json={"providers": [
            {"id": "c3", "issuer": "https://c.test/", "title": "C3"},
            {"id": "x3", "issuer": "https://x.test", "title": "X3"},
            {"id": "y3", "issuer": "https://y.test/", "title": "Y3"},
        ]})

        multi_backend_connection = MultiBackendConnection({"b1": domain1, "b2": domain2, "b3": domain3})

        assert multi_backend_connection.get_oidc_providers() == [
            OidcProvider(id="x1", issuer="https://x.test", title="X1", scopes=["openid"]),
            OidcProvider(id="y1", issuer="https://y.test", title="Y1", scopes=["openid"]),
        ]

        def get_me(request: requests.Request, context):
            auth = request.headers.get("Authorization")
            return {"user_id": auth}

        requests_mock.get("https://b1.test/v1/me", json=get_me)
        requests_mock.get("https://b2.test/v1/me", json=get_me)
        requests_mock.get("https://b3.test/v1/me", json=get_me)

        # Fake aggregator request containing bearer token for aggregator providers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/x1/yadayadayada"})

        con1 = multi_backend_connection.get_connection("b1")
        with con1.authenticated_from_request(request=request):
            assert con1.get("/me").json() == {"user_id": "Bearer oidc/x1/yadayadayada"}

        con2 = multi_backend_connection.get_connection("b2")
        with con2.authenticated_from_request(request=request):
            assert con2.get("/me").json() == {"user_id": "Bearer oidc/x2/yadayadayada"}

        con3 = multi_backend_connection.get_connection("b3")
        with con3.authenticated_from_request(request=request):
            assert con3.get("/me").json() == {"user_id": "Bearer oidc/x3/yadayadayada"}
