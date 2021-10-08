import logging
import types

import flask
import pytest
import requests

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
from openeo.rest.auth.auth import BearerAuth
from openeo_aggregator.config import CONNECTION_TIMEOUT_DEFAULT
from openeo_aggregator.connection import BackendConnection, MultiBackendConnection, LockedAuthException
from openeo_driver.backend import OidcProvider
from openeo_driver.errors import AuthenticationRequiredException


class TestBackendConnection:

    def test_plain_basic_auth_fails(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        requests_mock.get("https://foo.test/credentials/basic", json={"access_token": "3nt3r"})
        con = BackendConnection(id="foo", url="https://foo.test")
        with pytest.raises(LockedAuthException):
            con.authenticate_basic("john", "j0hn")

    def test_basic_auth_from_request(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})

        def get_me(request: requests.Request, context):
            if request.headers.get("Authorization") == "Bearer basic//l3tm31n":
                return {"user_id": "john"}
            else:
                context.status_code = 401
                return AuthenticationRequiredException().to_dict()

        requests_mock.get("https://foo.test/me", json=get_me)

        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer basic//l3tm31n"})
        assert con.auth is None
        with con.authenticated_from_request(request=request):
            assert isinstance(con.auth, BearerAuth)
            assert con.get("/me", expected_status=200).json() == {"user_id": "john"}
        assert con.auth is None

        with pytest.raises(OpenEoApiError, match=r"\[401\] AuthenticationRequired: Unauthorized"):
            con.get("/me")

    @pytest.mark.parametrize("exception", [Exception, ValueError, OpenEoApiError])
    def test_basic_auth_from_request_failure(self, requests_mock, exception):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer basic//l3tm31n"})
        assert con.auth is None
        with pytest.raises(exception):
            with con.authenticated_from_request(request=request):
                assert isinstance(con.auth, BearerAuth)
                raise exception
        # auth should be reset even with exception in `authenticated_from_request` body
        assert con.auth is None

    def test_plain_oidc_auth_fails(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        requests_mock.get("https://foo.test/credentials/oidc", json={"providers": [
            {"id": "fid", "issuer": "https://oidc.foo.test", "title": "Foo ID"},
        ]})
        requests_mock.get("https://oidc.foo.test/.well-known/openid-configuration", json={
            "token_endpoint": "https://oidc.foo.test/token",
            "userinfo_endpoint": "https://oidc.foo.test/userinfo",
        })
        requests_mock.post("https://oidc.foo.test/token", json={"access_token": "3nt3r"})
        con = BackendConnection(id="foo", url="https://foo.test")
        with pytest.raises(LockedAuthException):
            con.authenticate_oidc_refresh_token(client_id="cl13nt", refresh_token="r3fr35")

    def test_oidc_auth_from_request(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})

        def get_me(request: requests.Request, context):
            if request.headers.get("Authorization") == "Bearer oidc/fid/l3tm31n":
                return {"user_id": "john"}
            else:
                context.status_code = 401
                return AuthenticationRequiredException().to_dict()

        requests_mock.get("https://foo.test/me", json=get_me)

        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/fid/l3tm31n"})
        assert con.auth is None
        with con.authenticated_from_request(request=request):
            assert isinstance(con.auth, BearerAuth)
            assert con.get("/me", expected_status=200).json() == {"user_id": "john"}
        assert con.auth is None

        with pytest.raises(OpenEoApiError, match=r"\[401\] AuthenticationRequired: Unauthorized"):
            con.get("/me")

    @pytest.mark.parametrize("exception", [Exception, ValueError, OpenEoApiError])
    def test_oidc_auth_from_request_failure(self, requests_mock, exception):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/fid/l3tm31n"})
        assert con.auth is None
        with pytest.raises(exception):
            with con.authenticated_from_request(request=request):
                assert isinstance(con.auth, BearerAuth)
                raise exception
        # auth should be reset even with exception in `authenticated_from_request` body
        assert con.auth is None

    def test_override_default_timetout(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test")
        assert con.default_timeout == CONNECTION_TIMEOUT_DEFAULT
        with con.override(default_timeout=67):
            assert con.default_timeout == 67
        assert con.default_timeout == CONNECTION_TIMEOUT_DEFAULT

    def test_override_default_headers(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})

        def handler(request, context):
            return "The UA is " + request.headers["User-Agent"]

        requests_mock.get("https://foo.test/ua", text=handler)

        con = BackendConnection(id="foo", url="https://foo.test")
        assert con.get("/ua").text.startswith("The UA is openeo-aggregator/")
        with con.override(default_headers={"User-Agent": "Foobur 1.2"}):
            assert con.get("/ua").text == "The UA is Foobur 1.2"
        assert con.get("/ua").text.startswith("The UA is openeo-aggregator/")


class TestMultiBackendConnection:

    # TODO test version discovery in constructor

    @pytest.mark.parametrize(["bid1", "bid2"], [
        ("b1", "b1-dev"), ("b1", "b1.dev"), ("b1", "b1:dev"),
        ("AA", "BB")
    ])
    def test_backend_id_format_invalid(self, backend1, backend2, bid1, bid2):
        with pytest.raises(ValueError, match="should be alphanumeric only"):
            _ = MultiBackendConnection({bid1: backend1, bid2: backend2})

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

    @pytest.mark.parametrize(["pid", "issuer", "title"], [
        ("egi", "https://egi.test", "EGI"),
        ("agg-egi", "https://EGI.test/", "Agg EGI"),
    ])
    def test_build_oidc_handling_basic(self, multi_backend_connection, backend1, backend2, pid, issuer, title):
        providers = multi_backend_connection.build_oidc_handling(configured_providers=[
            OidcProvider(id=pid, issuer=issuer, title=title),
            OidcProvider(id="egi-dev", issuer="https://egi-dev.test", title="EGI dev"),
        ])
        assert providers == [
            OidcProvider(id=pid, issuer=issuer, title=title, scopes=["openid"]),
        ]

        for con in multi_backend_connection:
            assert con._oidc_provider_map == {pid: "egi"}

    @pytest.mark.parametrize(["issuer_y1", "issuer_y2"], [
        ("https://y.test", "https://y.test"),
        ("https://y.test", "https://y.test/"),
        ("https://y.test/", "https://y.test/"),
    ])
    def test_build_oidc_handling_intersection(
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

        providers = multi_backend_connection.build_oidc_handling(configured_providers=[
            OidcProvider("xa", "https://x.test", "A-X"),
            OidcProvider("ya", "https://y.test", "A-Y"),
            OidcProvider("za", "https://z.test", "A-Z"),
        ])
        assert providers == [
            OidcProvider(id="ya", issuer="https://y.test", title="A-Y", scopes=["openid"]),
        ]

        assert [con._oidc_provider_map for con in multi_backend_connection] == [
            {"ya": "y1"},
            {"ya": "y2"},
        ]

    def test_build_oidc_handling_order(
            self, multi_backend_connection, requests_mock, backend1, backend2
    ):
        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "d1", "issuer": "https://d.test", "title": "D1"},
            {"id": "b1", "issuer": "https://b.test", "title": "B1"},
            {"id": "c1", "issuer": "https://c.test/", "title": "C1"},
            {"id": "a1", "issuer": "https://a.test", "title": "A1"},
            {"id": "e1", "issuer": "https://e.test/", "title": "E1"},
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "e2", "issuer": "https://e.test", "title": "E2"},
            {"id": "b2", "issuer": "https://b.test/", "title": "B2"},
            {"id": "c2", "issuer": "https://c.test", "title": "C2"},
            {"id": "a2", "issuer": "https://a.test", "title": "A2"},
            {"id": "d2", "issuer": "https://d.test", "title": "D2"},
        ]})

        providers = multi_backend_connection.build_oidc_handling(configured_providers=[
            OidcProvider("a-b", "https://b.test", "A-B"),
            OidcProvider("a-e", "https://e.test/", "A-E"),
            OidcProvider("a-a", "https://a.test", "A-A"),
            OidcProvider("a-d", "https://d.test", "A-D"),
            OidcProvider("a-c", "https://c.test/", "A-C"),
        ])
        assert [p.issuer for p in providers] == [
            "https://b.test", "https://e.test/", "https://a.test", "https://d.test", "https://c.test/"
        ]
        assert [con._oidc_provider_map for con in multi_backend_connection] == [
            {'a-a': 'a1', 'a-b': 'b1', 'a-c': 'c1', 'a-d': 'd1', 'a-e': 'e1'},
            {'a-a': 'a2', 'a-b': 'b2', 'a-c': 'c2', 'a-d': 'd2', 'a-e': 'e2'},
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

        assert multi_backend_connection.build_oidc_handling(configured_providers=[
            OidcProvider("ax", "https://x.test", "A-X"),
            OidcProvider("ay", "https://y.test", "A-Y"),
        ]) == [
            OidcProvider(id="ax", issuer="https://x.test", title="A-X", scopes=["openid"]),
            OidcProvider(id="ay", issuer="https://y.test", title="A-Y", scopes=["openid"]),
        ]

        def get_me(request: requests.Request, context):
            auth = request.headers.get("Authorization")
            return {"user_id": auth}

        requests_mock.get("https://b1.test/v1/me", json=get_me)
        requests_mock.get("https://b2.test/v1/me", json=get_me)
        requests_mock.get("https://b3.test/v1/me", json=get_me)

        # Fake aggregator request containing bearer token for aggregator providers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ax/yadayadayada"})

        con1 = multi_backend_connection.get_connection("b1")
        with con1.authenticated_from_request(request=request):
            assert con1.get("/me").json() == {"user_id": "Bearer oidc/x1/yadayadayada"}

        con2 = multi_backend_connection.get_connection("b2")
        with con2.authenticated_from_request(request=request):
            assert con2.get("/me").json() == {"user_id": "Bearer oidc/x2/yadayadayada"}

        con3 = multi_backend_connection.get_connection("b3")
        with con3.authenticated_from_request(request=request):
            assert con3.get("/me").json() == {"user_id": "Bearer oidc/x3/yadayadayada"}

    def test_oidc_provider_mapping_changes(self, requests_mock, caplog):
        domain1 = "https://b1.test/v1"
        requests_mock.get(domain1 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(domain1 + "/credentials/oidc", json={"providers": [
            {"id": "x1", "issuer": "https://x.test/", "title": "X1"},
        ]})

        def get_me(request: requests.Request, context):
            auth = request.headers.get("Authorization")
            return {"user_id": auth}

        requests_mock.get("https://b1.test/v1/me", json=get_me)

        multi_backend_connection = MultiBackendConnection({"b1": domain1})
        con1 = multi_backend_connection.get_connection("b1")

        configured_providers = [
            OidcProvider("ax", "https://x.test", "A-X"),
            OidcProvider("ay", "https://y.test", "A-Y"),
        ]

        agg_providers = multi_backend_connection.build_oidc_handling(configured_providers)
        assert agg_providers == [OidcProvider(id="ax", issuer="https://x.test", title="A-X", scopes=["openid"])]
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert warnings == ""

        # Fake aggregator request containing bearer token for aggregator providers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ax/yadayadayada"})
        with con1.authenticated_from_request(request=request):
            assert con1.get("/me").json() == {"user_id": "Bearer oidc/x1/yadayadayada"}

        # Change backend's oidc config, clear its cache and rebuild OIDC handling
        requests_mock.get(domain1 + "/credentials/oidc", json={"providers": [
            {"id": "y1", "issuer": "https://y.test/", "title": "Y1"},
        ]})
        multi_backend_connection._cache.flush_all()
        agg_providers = multi_backend_connection.build_oidc_handling(configured_providers)
        assert agg_providers == [OidcProvider(id="ay", issuer="https://y.test", title="A-Y", scopes=["openid"])]
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert "Changing OIDC provider mapping" in warnings

        # Try old auth headers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ax/yadayadayada"})
        with con1.authenticated_from_request(request=request):
            assert con1.get("/me").json() == {"user_id": "Bearer oidc/ax/yadayadayada"}

        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert "OIDC provider mapping failure" in warnings

        # New headers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ay/yadayadayada"})
        with con1.authenticated_from_request(request=request):
            assert con1.get("/me").json() == {"user_id": "Bearer oidc/y1/yadayadayada"}
