import logging
import types

import flask
import pytest
import requests
from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
from openeo.rest.auth.auth import BearerAuth
from openeo_driver.backend import OidcProvider
from openeo_driver.errors import AuthenticationRequiredException, OpenEOApiException
from openeo_driver.testing import DictSubSet

from openeo_aggregator.caching import DictMemoizer
from openeo_aggregator.config import CONNECTION_TIMEOUT_DEFAULT, get_backend_config
from openeo_aggregator.connection import (
    BackendConnection,
    InvalidatedConnection,
    LockedAuthException,
    MultiBackendConnection,
)
from openeo_aggregator.testing import clock_mock, config_overrides


class TestBackendConnection:
    def test_plain_basic_auth_fails(self, requests_mock):
        requests_mock.get(
            "https://foo.test/",
            json={"api_version": "1.0.0", "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}]},
        )
        requests_mock.get("https://foo.test/credentials/basic", json={"access_token": "3nt3r"})
        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
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

        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer basic//l3tm31n"})
        assert con.auth is None
        with con.authenticated_from_request(request=request):
            assert isinstance(con.auth, BearerAuth)
            assert con.get("/me", expected_status=200).json() == {"user_id": "john"}
        assert con.auth is None

        with pytest.raises(OpenEoApiError, match=r"\[401\] AuthenticationRequired: Unauthorized"):
            con.get("/me")

    @pytest.mark.parametrize(
        "exception",
        [
            Exception,
            ValueError,
            OpenEoApiError(http_status_code=500, code="Internal", message="Nope"),
        ],
    )
    def test_basic_auth_from_request_failure(self, requests_mock, exception):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer basic//l3tm31n"})
        assert con.auth is None
        with pytest.raises(exception.__class__ if isinstance(exception, Exception) else exception):
            with con.authenticated_from_request(request=request):
                assert isinstance(con.auth, BearerAuth)
                raise exception
        # auth should be reset even with exception in `authenticated_from_request` body
        assert con.auth is None

    def test_plain_oidc_auth_fails(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        requests_mock.get(
            "https://foo.test/credentials/oidc",
            json={
                "providers": [
                    {"id": "egi", "issuer": "https://egi.test", "title": "EGI"},
                ]
            },
        )
        requests_mock.get(
            "https://egi.test/.well-known/openid-configuration",
            json={
                "token_endpoint": "https://egi.test/token",
                "userinfo_endpoint": "https://egi.test/userinfo",
            },
        )
        requests_mock.post("https://egi.test/token", json={"access_token": "3nt3r"})
        con = BackendConnection(id="foo", url="https://foo.test")
        with pytest.raises(LockedAuthException):
            con.authenticate_oidc_refresh_token(client_id="cl13nt", refresh_token="r3fr35")

    @pytest.mark.parametrize("backend_pid", ["egi", "aho"])
    def test_oidc_auth_from_request(self, requests_mock, backend_pid):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        requests_mock.get(
            "https://foo.test/credentials/oidc",
            json={
                "providers": [
                    {"id": backend_pid, "issuer": "https://egi.test", "title": "EGI"},
                ]
            },
        )

        def get_me(request: requests.Request, context):
            if request.headers.get("Authorization") == f"Bearer oidc/{backend_pid}/l3tm31n":
                return {"user_id": "john"}
            else:
                context.status_code = 401
                return AuthenticationRequiredException().to_dict()

        requests_mock.get("https://foo.test/me", json=get_me)

        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/egi/l3tm31n"})
        assert con.auth is None
        with con.authenticated_from_request(request=request):
            assert isinstance(con.auth, BearerAuth)
            assert con.get("/me", expected_status=200).json() == {"user_id": "john"}
        assert con.auth is None

        with pytest.raises(OpenEoApiError, match=r"\[401\] AuthenticationRequired: Unauthorized"):
            con.get("/me")

    @pytest.mark.parametrize(
        "exception",
        [
            Exception,
            ValueError,
            OpenEoApiError(http_status_code=500, code="Internal", message="Nope"),
        ],
    )
    def test_oidc_auth_from_request_failure(self, requests_mock, exception):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        requests_mock.get(
            "https://foo.test/credentials/oidc",
            json={
                "providers": [
                    {"id": "egi", "issuer": "https://egi.test", "title": "EGI"},
                ]
            },
        )

        con = BackendConnection(id="foo", url="https://foo.test")
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/egi/l3tm31n"})
        assert con.auth is None
        with pytest.raises(exception.__class__ if isinstance(exception, Exception) else exception):
            with con.authenticated_from_request(request=request):
                assert isinstance(con.auth, BearerAuth)
                raise exception
        # auth should be reset even with exception in `authenticated_from_request` body
        assert con.auth is None

    @pytest.mark.parametrize("fail", [False, True])
    def test_override_default_timeout(self, requests_mock, fail):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
        assert con.default_timeout == CONNECTION_TIMEOUT_DEFAULT
        try:
            with con.override(default_timeout=67):
                assert con.default_timeout == 67
                if fail:
                    raise RuntimeError
        except RuntimeError:
            pass
        assert con.default_timeout == CONNECTION_TIMEOUT_DEFAULT

    @pytest.mark.parametrize("fail", [False, True])
    def test_override_default_headers(self, requests_mock, fail):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})

        def handler(request, context):
            return "The UA is " + request.headers["User-Agent"]

        requests_mock.get("https://foo.test/ua", text=handler)

        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
        assert con.get("/ua").text.startswith("The UA is openeo-aggregator/")
        try:
            with con.override(default_headers={"User-Agent": "Foobur 1.2"}):
                assert con.get("/ua").text == "The UA is Foobur 1.2"
                if fail:
                    raise RuntimeError
        except RuntimeError:
            pass
        assert con.get("/ua").text.startswith("The UA is openeo-aggregator/")

    def test_invalidate(self, requests_mock):
        requests_mock.get("https://foo.test/", json={"api_version": "1.0.0"})
        con = BackendConnection(id="foo", url="https://foo.test", configured_oidc_providers=[])
        assert con.get("/").json() == {"api_version": "1.0.0"}
        con.invalidate()
        with pytest.raises(InvalidatedConnection):
            con.get("/")

    def test_init_vs_default_timeout(self, requests_mock):
        def get_handler(expected_timeout: int):
            def capabilities(request, context):
                assert request.timeout == expected_timeout
                return {"api_version": "1.0.0"}

            return capabilities

        # Capabilities request during init
        m = requests_mock.get("https://foo.test/", json=get_handler(expected_timeout=5))
        con = BackendConnection(
            id="foo", url="https://foo.test", configured_oidc_providers=[], default_timeout=20, init_timeout=5
        )
        assert m.call_count == 1

        # Post-init capabilities request
        requests_mock.get("https://foo.test/", json=get_handler(expected_timeout=20))
        con.get("/")
        assert m.call_count == 1

    def test_version_discovery_timeout(self, requests_mock):
        well_known = requests_mock.get(
            "https://foo.test/.well-known/openeo",
            status_code=200,
            json={
                "versions": [
                    {"api_version": "1.0.0", "url": "https://oeo.test/v1/"},
                ],
            },
        )
        requests_mock.get("https://oeo.test/v1/", status_code=200, json={"api_version": "1.0.0"})

        _ = BackendConnection(
            id="foo", url="https://foo.test", configured_oidc_providers=[], default_timeout=20, init_timeout=5
        )
        assert well_known.call_count == 1
        assert well_known.request_history[-1].timeout == 5


class TestMultiBackendConnection:

    # TODO test version discovery in constructor

    def test_from_config(self, backend1, backend2):
        backends = MultiBackendConnection.from_config()
        assert set(c.id for c in backends.get_connections()) == {"b1", "b2"}

    @pytest.mark.parametrize(["bid1", "bid2"], [("b1", "b1-dev"), ("b1", "b1.dev"), ("b1", "b1:dev"), ("AA", "BB")])
    def test_backend_id_format_invalid(self, backend1, backend2, bid1, bid2):
        with pytest.raises(ValueError, match="should be alphanumeric only"):
            _ = MultiBackendConnection({bid1: backend1, bid2: backend2}, configured_oidc_providers=[])

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

    def test_get_federation_overview_basic(self, multi_backend_connection):
        assert multi_backend_connection.get_federation_overview() == {
            "b1": {
                "url": "https://b1.test/v1",
                "title": "Dummy Federation One",
                "description": "Welcome to Federation One.",
                "status": "online",
            },
            "b2": {
                "url": "https://b2.test/v1",
                "title": "Dummy The Second",
                "description": "Test instance of openEO Aggregator",
                "status": "online",
            },
        }

    def test_get_federation_overview_offline(self, multi_backend_connection, backend1, backend2, requests_mock):
        requests_mock.get(f"{backend2}/", status_code=500, json={"error": "nope"})
        assert multi_backend_connection.get_federation_overview() == {
            "b1": {
                "url": "https://b1.test/v1",
                "title": "Dummy Federation One",
                "description": "Welcome to Federation One.",
                "status": "online",
            },
            "b2": {
                "url": "https://b2.test/v1",
                "description": "Federated openEO backend 'b2'",
                "title": "Backend 'b2'",
                "status": "offline",
            },
        }

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {list}),
            ({"memoizer": {"type": "jsondict", "config": {"default_ttl": 66}}}, {bytes}),
        ],
    )
    def test_api_version(
        self,
        requests_mock,
        backend1,
        backend2,
        overrides,
        expected_cache_types,
        caplog,
    ):
        m1 = requests_mock.get(backend1 + "/", json={"api_version": "1.2.3"})
        m2 = requests_mock.get(backend2 + "/", json={"api_version": "1.2.3"})

        with config_overrides(**overrides):
            multi_backend_connection = MultiBackendConnection.from_config()

        assert (m1.call_count, m2.call_count) == (0, 0)
        v123 = ComparableVersion("1.2.3")
        assert multi_backend_connection.api_version_minimum == v123
        assert multi_backend_connection.api_version_maximum == v123
        assert (m1.call_count, m2.call_count) == (1, 1)
        assert multi_backend_connection.api_version_minimum == v123
        assert multi_backend_connection.api_version_maximum == v123
        assert (m1.call_count, m2.call_count) == (1, 1)
        with clock_mock(offset=70):
            assert multi_backend_connection.api_version_minimum == v123
            assert multi_backend_connection.api_version_maximum == v123
            assert (m1.call_count, m2.call_count) == (2, 2)

        assert caplog.messages == []

        assert isinstance(multi_backend_connection._memoizer, DictMemoizer)
        cache_dump = multi_backend_connection._memoizer.dump(values_only=True)
        assert set(type(v) for v in cache_dump) == expected_cache_types

    @pytest.mark.parametrize(
        ["overrides", "expected_cache_types"],
        [
            ({"memoizer": {"type": "dict"}}, {list}),
            ({"memoizer": {"type": "jsondict", "config": {"default_ttl": 66}}}, {bytes}),
        ],
    )
    def test_api_versions(
        self,
        requests_mock,
        backend1,
        backend2,
        overrides,
        expected_cache_types,
        caplog,
    ):
        m1 = requests_mock.get(backend1 + "/", json={"api_version": "1.2.3"})
        m2 = requests_mock.get(backend2 + "/", json={"api_version": "1.3.5"})

        with config_overrides(**overrides):
            multi_backend_connection = MultiBackendConnection.from_config()

        assert (m1.call_count, m2.call_count) == (0, 0)
        v123 = ComparableVersion("1.2.3")
        v135 = ComparableVersion("1.3.5")
        assert multi_backend_connection.api_version_minimum == v123
        assert multi_backend_connection.api_version_maximum == v135
        assert multi_backend_connection.get_api_versions() == {v123, v135}
        assert (m1.call_count, m2.call_count) == (1, 1)
        assert multi_backend_connection.api_version_minimum == v123
        assert multi_backend_connection.api_version_maximum == v135
        assert multi_backend_connection.get_api_versions() == {v123, v135}
        assert (m1.call_count, m2.call_count) == (1, 1)
        with clock_mock(offset=70):
            assert multi_backend_connection.api_version_minimum == v123
            assert multi_backend_connection.api_version_maximum == v135
            assert multi_backend_connection.get_api_versions() == {v123, v135}
            assert (m1.call_count, m2.call_count) == (2, 2)

        assert caplog.messages == []

        assert isinstance(multi_backend_connection._memoizer, DictMemoizer)
        cache_dump = multi_backend_connection._memoizer.dump(values_only=True)
        assert set(type(v) for v in cache_dump) == expected_cache_types

    @pytest.mark.parametrize(
        ["pid", "issuer", "title"],
        [
            ("egi", "https://egi.test", "EGI"),
            ("agg-egi", "https://EGI.test/", "Agg EGI"),
        ],
    )
    def test_build_oidc_handling_basic(self, pid, issuer, title):
        multi_backend_connection = MultiBackendConnection(
            backends=get_backend_config().aggregator_backends,
            configured_oidc_providers=[
                OidcProvider(id=pid, issuer=issuer, title=title),
                OidcProvider(id="egi-dev", issuer="https://egi-dev.test", title="EGI dev"),
            ],
        )

        for con in multi_backend_connection:
            assert con.get_oidc_provider_map() == {pid: "egi"}

    @pytest.mark.parametrize(
        ["issuer_y1", "issuer_y2"],
        [
            ("https://y.test", "https://y.test"),
            ("https://y.test", "https://y.test/"),
            ("https://y.test/", "https://y.test/"),
        ],
    )
    def test_build_oidc_handling_intersection(self, requests_mock, backend1, backend2, issuer_y1, issuer_y2):
        requests_mock.get(
            backend1 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "x1", "issuer": "https://x.test", "title": "X1"},
                    {"id": "y1", "issuer": issuer_y1, "title": "YY1"},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "y2", "issuer": issuer_y2, "title": "YY2"},
                    {"id": "z2", "issuer": "https://z.test", "title": "ZZZ2"},
                ]
            },
        )

        multi_backend_connection = MultiBackendConnection(
            backends=get_backend_config().aggregator_backends,
            configured_oidc_providers=[
                OidcProvider("xa", "https://x.test", "A-X"),
                OidcProvider("ya", "https://y.test", "A-Y"),
                OidcProvider("za", "https://z.test", "A-Z"),
            ],
        )

        assert [con.get_oidc_provider_map() for con in multi_backend_connection] == [
            {"xa": "x1", "ya": "y1"},
            {"ya": "y2", "za": "z2"},
        ]

    def test_build_oidc_handling_intersection_empty(self, requests_mock, backend1, backend2):
        requests_mock.get(
            backend1 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "x1", "issuer": "https://x.test", "title": "X1"},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "y2", "issuer": "https://y.test", "title": "YY2"},
                ]
            },
        )

        multi_backend_connection = MultiBackendConnection(
            backends=get_backend_config().aggregator_backends,
            configured_oidc_providers=[
                OidcProvider("ya", "https://y.test", "A-Y"),
                OidcProvider("za", "https://z.test", "A-Z"),
            ],
        )

        assert [con.get_oidc_provider_map() for con in multi_backend_connection] == [
            {},
            {"ya": "y2"},
        ]

    def test_build_oidc_handling_order(self, requests_mock, backend1, backend2):
        requests_mock.get(
            backend1 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "d1", "issuer": "https://d.test", "title": "D1"},
                    {"id": "b1", "issuer": "https://b.test", "title": "B1"},
                    {"id": "c1", "issuer": "https://c.test/", "title": "C1"},
                    {"id": "a1", "issuer": "https://a.test", "title": "A1"},
                    {"id": "e1", "issuer": "https://e.test/", "title": "E1"},
                ]
            },
        )
        requests_mock.get(
            backend2 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "e2", "issuer": "https://e.test", "title": "E2"},
                    {"id": "b2", "issuer": "https://b.test/", "title": "B2"},
                    {"id": "c2", "issuer": "https://c.test", "title": "C2"},
                    {"id": "a2", "issuer": "https://a.test", "title": "A2"},
                    {"id": "d2", "issuer": "https://d.test", "title": "D2"},
                ]
            },
        )

        multi_backend_connection = MultiBackendConnection(
            backends=get_backend_config().aggregator_backends,
            configured_oidc_providers=[
                OidcProvider("a-b", "https://b.test", "A-B"),
                OidcProvider("a-e", "https://e.test/", "A-E"),
                OidcProvider("a-a", "https://a.test", "A-A"),
                OidcProvider("a-d", "https://d.test", "A-D"),
                OidcProvider("a-c", "https://c.test/", "A-C"),
            ],
        )

        assert [con.get_oidc_provider_map() for con in multi_backend_connection] == [
            {"a-a": "a1", "a-b": "b1", "a-c": "c1", "a-d": "d1", "a-e": "e1"},
            {"a-a": "a2", "a-b": "b2", "a-c": "c2", "a-d": "d2", "a-e": "e2"},
        ]

    def test_oidc_provider_mapping(self, requests_mock):
        domain1 = "https://b1.test/v1"
        requests_mock.get(domain1 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(
            domain1 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "a1", "issuer": "https://a.test/", "title": "A1"},
                    {"id": "x1", "issuer": "https://x.test/", "title": "X1"},
                    {"id": "y1", "issuer": "https://y.test/", "title": "Y1"},
                ]
            },
        )
        domain2 = "https://b2.test/v1"
        requests_mock.get(domain2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(
            domain2 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "b2", "issuer": "https://b.test", "title": "B2"},
                    {"id": "x2", "issuer": "https://x.test", "title": "X2"},
                    {"id": "y2", "issuer": "https://y.test", "title": "Y2"},
                ]
            },
        )
        domain3 = "https://b3.test/v1"
        requests_mock.get(domain3 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(
            domain3 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "c3", "issuer": "https://c.test/", "title": "C3"},
                    {"id": "x3", "issuer": "https://x.test", "title": "X3"},
                    {"id": "y3", "issuer": "https://y.test/", "title": "Y3"},
                ]
            },
        )

        multi_backend_connection = MultiBackendConnection(
            backends={"b1": domain1, "b2": domain2, "b3": domain3},
            configured_oidc_providers=[
                OidcProvider("ax", "https://x.test", "A-X"),
                OidcProvider("ay", "https://y.test", "A-Y"),
            ],
        )

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
        requests_mock.get(
            domain1 + "/credentials/oidc",
            json={
                "providers": [
                    {"id": "x1", "issuer": "https://x.test/", "title": "X1"},
                ]
            },
        )

        def get_me(request: requests.Request, context):
            auth = request.headers.get("Authorization")
            return {"user_id": auth}

        requests_mock.get("https://b1.test/v1/me", json=get_me)

        configured_oidc_providers = [
            OidcProvider("ax", "https://x.test", "A-X"),
            OidcProvider("ay", "https://y.test", "A-Y"),
        ]

        multi_backend_connection = MultiBackendConnection(
            backends={"b1": domain1}, configured_oidc_providers=configured_oidc_providers
        )

        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)
        assert warnings == ""

        # Fake aggregator request containing bearer token for aggregator providers
        request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ax/yadayadayada"})
        with multi_backend_connection.get_connection("b1").authenticated_from_request(request=request) as con:
            assert con.get("/me").json() == {"user_id": "Bearer oidc/x1/yadayadayada"}

        # Change backend's oidc config, wait for connections cache to expire
        with clock_mock(offset=1000):
            requests_mock.get(
                domain1 + "/credentials/oidc",
                json={
                    "providers": [
                        {"id": "y1", "issuer": "https://y.test/", "title": "Y1"},
                    ]
                },
            )
            # Try old auth headers
            request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ax/yadayadayada"})
            with pytest.raises(OpenEOApiException, match="Back-end 'b1' does not support OIDC provider 'ax'"):
                with multi_backend_connection.get_connection("b1").authenticated_from_request(request=request):
                    pass
                errors = "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)
                assert "lacks OIDC provider support: 'ax' not in {'ay': 'y1'}." in errors

            # New headers
            request = flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/ay/yadayadayada"})
            with multi_backend_connection.get_connection("b1").authenticated_from_request(request=request) as con:
                assert con.get("/me").json() == {"user_id": "Bearer oidc/y1/yadayadayada"}

    def test_connection_invalidate(self, backend1):
        multi_backend_connection = MultiBackendConnection(backends={"b1": backend1}, configured_oidc_providers=[])

        con1 = multi_backend_connection.get_connection("b1")
        assert con1.get("/").json() == DictSubSet({"api_version": "1.1.0"})

        # Wait for connections cache to expire
        with clock_mock(offset=1000):
            con2 = multi_backend_connection.get_connection("b1")

            with pytest.raises(InvalidatedConnection):
                con1.get("/")
            assert con2.get("/").json() == DictSubSet({"api_version": "1.1.0"})

    def test_get_connections(self, requests_mock, backend1, backend2):
        multi_backend_connection = MultiBackendConnection(
            backends={"b1": backend1, "b2": backend2}, configured_oidc_providers=[]
        )

        assert set(b.id for b in multi_backend_connection.get_connections()) == {"b1", "b2"}
        assert multi_backend_connection.get_disabled_connection_ids() == set()

        # Wait for connections cache to expire
        with clock_mock(offset=1000):
            requests_mock.get(backend1 + "/", status_code=500, json={"error": "nope"})

            assert set(b.id for b in multi_backend_connection.get_connections()) == {"b2"}
            assert multi_backend_connection.get_disabled_connection_ids() == {"b1"}
