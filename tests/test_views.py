import itertools
import logging
import re
from typing import Tuple

import pytest
import requests

from openeo_aggregator.backend import AggregatorCollectionCatalog
from openeo_aggregator.config import AggregatorConfig
from openeo_aggregator.connection import MultiBackendConnection
from openeo_driver.errors import JobNotFoundException, JobNotFinishedException, \
    ProcessGraphInvalidException
from openeo_driver.testing import ApiTester, TEST_USER_AUTH_HEADER, TEST_USER, TEST_USER_BEARER_TOKEN, DictSubSet
from .conftest import assert_dict_subset, get_api100, get_flask_app


class TestGeneral:
    def test_capabilities(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        assert capabilities["api_version"] == "1.0.0"
        endpoints = capabilities["endpoints"]
        assert {"methods": ["GET"], "path": "/collections"} in endpoints
        assert {"methods": ["GET"], "path": "/collections/{collection_id}"} in endpoints
        assert {"methods": ["GET"], "path": "/processes"} in endpoints
        assert capabilities["federation"] == {
            "b1": {"url": "https://b1.test/v1"},
            "b2": {"url": "https://b2.test/v1"},
        }

    def test_billing_plans(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        billing = capabilities["billing"]
        assert billing["currency"] == "EUR"
        plans = {p["name"]: p for p in billing["plans"]}
        assert "early-adopter" in plans
        assert plans["early-adopter"]["paid"] is True

    def test_only_oidc_auth(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert {e for e in endpoints if e.startswith("/credentials")} == {"/credentials/oidc"}

    def test_info(self, flask_app):
        api100 = ApiTester(api_version="1.0.0", client=flask_app.test_client(), url_root="/")
        res = api100.get("_info").assert_status_code(200)
        assert res.json == {
            "backends": [{"id": "b1", "root_url": "https://b1.test/v1"}, {"id": "b2", "root_url": "https://b2.test/v1"}]
        }

    def test_health_check_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", text="OK")
        resp = api100.get("/health").assert_status_code(200)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": 200,
        }

    @pytest.mark.parametrize(["status_code"], [(404,), (500,)])
    def test_health_check_failed_backend(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", status_code=status_code, text="broken")
        resp = api100.get("/health").assert_status_code(status_code)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {"status_code": status_code, "text": "broken", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": status_code,
        }

    def test_health_check_invalid_backend(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", text='Inva{id J}0n', headers={"Content-type": "application/json"})
        resp = api100.get("/health").assert_status_code(500)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {
                    "status_code": 200,
                    "error": "JSONDecodeError('Expecting value: line 1 column 1 (char 0)',)",
                    "response_time": pytest.approx(0.1, abs=0.1),
                    "error_time": pytest.approx(0.1, abs=0.1),
                },
            },
            "status_code": 500,
        }


class TestCatalog:

    def test_collections_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == {"S1", "S2", "S3"}

    def test_collections_duplicate(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S3"}]})
        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == {"S1", "S2", "S3"}

    def test_collection_full_metadata(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S1", json={"id": "S1", "title": "b1 S1"})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1 S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend2 + "/collections/S3", json={"id": "S3", "title": "b2 S3"})

        res = api100.get("/collections/S1").assert_status_code(200).json
        assert res == DictSubSet({"id": "S1", "title": "b1 S1"})

        res = api100.get("/collections/S2").assert_status_code(200).json
        assert res == DictSubSet({"id": "S2", "title": "b1 S2"})

        res = api100.get("/collections/S3").assert_status_code(200).json
        assert res == DictSubSet({"id": "S3", "title": "b2 S3"})

        res = api100.get("/collections/S4")
        res.assert_error(404, "CollectionNotFound")

    def test_collection_items(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        def collection_items(request, context):
            assert request.qs == {"bbox": ["5,45,20,50"], "datetime": ["2019-09-20/2019-09-22"], "limit": ["2"]}
            context.headers["Content-Type"] = "application/json"
            return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": "blabla"}]}

        requests_mock.get(backend1 + "/collections/S1/items", json=collection_items)

        res = api100.get("/collections/S1/items?bbox=5,45,20,50&datetime=2019-09-20/2019-09-22&limit=2")
        res.assert_status_code(200)
        assert res.json == {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": "blabla"}]}

    @pytest.mark.parametrize(["backend1_up", "backend2_up", "expected"], [
        (True, False, {"S1", "S2"}),
        (False, True, {"S3"}),
        (False, False, set()),
    ])
    def test_collections_resilience(
            self, api100, requests_mock, backend1, backend2, backend1_up, backend2_up, expected
    ):
        if backend1_up:
            requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        else:
            requests_mock.get(backend1 + "/collections", status_code=404, text="down")
        if backend2_up:
            requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        else:
            requests_mock.get(backend2 + "/collections", status_code=404, text="down")

        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == expected
        # TODO: test caching of results

    @pytest.mark.parametrize("status_code", [204, 303, 404, 500])
    def test_collection_full_metadata_resilience(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend1 + "/collections/S1", json={"id": "S1", "title": "b1 S1"})
        requests_mock.get(backend1 + "/collections/S2", status_code=status_code, text="down")
        requests_mock.get(backend2 + "/collections/S3", status_code=status_code, text="down")

        res = api100.get("/collections/S1").assert_status_code(200).json
        assert res == DictSubSet({"id": "S1", "title": "b1 S1"})

        api100.get("/collections/S2").assert_error(404, "CollectionNotFound")
        api100.get("/collections/S3").assert_error(404, "CollectionNotFound")
        api100.get("/collections/S4").assert_error(404, "CollectionNotFound")
        # TODO: test caching of results


class TestAuthentication:
    def test_credentials_oidc_default(self, api100, backend1, backend2):
        res = api100.get("/credentials/oidc").assert_status_code(200).json
        assert res == {"providers": [
            {"id": "egi", "issuer": "https://egi.test", "title": "EGI", "scopes": ["openid"]}
        ]}

    def test_credentials_oidc_intersection(self, requests_mock, config, backend1, backend2):
        # When mocking `/credentials/oidc` we have to do that before build flask app
        # because it's requested during app building (through `HttpAuthHandler`),
        # so unlike other tests we can not use fixtures that build the app/client/api automatically
        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "x", "issuer": "https://x.test", "title": "X"},
            {"id": "y", "issuer": "https://y.test", "title": "YY"},
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "y", "issuer": "https://y.test", "title": "YY"},
            {"id": "z", "issuer": "https://z.test", "title": "ZZZ"},
        ]})
        # Manually creating app and api100 (which we do with fixtures elsewhere)
        api100 = get_api100(get_flask_app(config))

        res = api100.get("/credentials/oidc").assert_status_code(200).json
        assert res == {"providers": [
            {"id": "y-agg", "issuer": "https://y.test", "title": "Y (agg)", "scopes": ["openid"]}
        ]}

    def test_me_unauthorized(self, api100):
        api100.get("/me").assert_error(401, "AuthenticationRequired")

    def test_me_basic_auth_invalid(self, api100):
        headers = {"Authorization": "Bearer " + "basic//foobar"}
        api100.get("/me", headers=headers).assert_error(403, "TokenInvalid")

    def test_me_basic_auth(self, api100):
        headers = TEST_USER_AUTH_HEADER
        res = api100.get("/me", headers=headers).assert_status_code(200)
        assert res.json["user_id"] == TEST_USER


class TestAuthEntitlementCheck:
    def test_basic_auth(self, api100_with_entitlement_check, caplog):
        api100_with_entitlement_check.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="An EGI account is required for using openEO Platform."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"internal_auth_data.*authentication_method.*basic", warnings)

    def test_oidc_no_entitlement_data(self, api100_with_entitlement_check, requests_mock, caplog):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="Proper enrollment in openEO Platform virtual organization is required."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"KeyError.*eduperson_entitlement", warnings)

    def test_oidc_no_early_adopter(self, api100_with_entitlement_check, requests_mock, caplog):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {
                "sub": "john",
                "eduperson_entitlement": [
                    "urn:mace:egi.eu:group:vo.openeo.test:role=foo#test",
                    "urn:mace:egi.eu:group:vo.openeo.test:role=member#test",
                ],
            }

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="Proper enrollment in openEO Platform virtual organization is required."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"user_id.*john", warnings)
        assert re.search(r"eduperson_entitlements.*vo\.openeo\.test:role=foo", warnings)

    @pytest.mark.parametrize(["eduperson_entitlement", "expected_roles", "expected_plan"], [
        (["urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu"], ["FreeTier"], "free"),
        (["urn:mace:egi.eu:group:vo.openeo.cloud:role=meh#aai.egi.eu"], ["FreeTier"], "free"),
        (
                [
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=foo#aai.egi.eu",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu",
                ],
                ["EarlyAdopter"], "early-adopter",
        )
    ])
    def test_oidc_enrolled(
            self, api100_with_entitlement_check, requests_mock,
            eduperson_entitlement, expected_roles, expected_plan,
    ):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {
                "sub": "john",
                "eduperson_entitlement": eduperson_entitlement
            }

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me").assert_status_code(200)
        data = res.json
        assert data["user_id"] == "john"
        assert data["info"]["roles"] == expected_roles
        assert data["default_plan"] == expected_plan


class TestProcessing:
    def test_processes_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        res = api100.get("/processes").assert_status_code(200).json
        assert res == {
            "processes": [
                {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
                {"id": "mean", "parameters": [{"name": "data"}]},
                {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            ],
            "links": [],
        }

    @pytest.mark.parametrize(["backend1_up", "backend2_up", "expected"], [
        (True, False, [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]),
        (False, True, [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]),
        (False, False, []),
    ])
    def test_processes_resilience(self, api100, requests_mock, backend1, backend2, backend1_up, backend2_up, expected):
        if backend1_up:
            requests_mock.get(backend1 + "/processes", json={"processes": [
                {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
                {"id": "mean", "parameters": [{"name": "data"}]},
            ]})
        else:
            requests_mock.get(backend1 + "/processes", status_code=404, text="nope")
        if backend2_up:
            requests_mock.get(backend2 + "/processes", json={"processes": [
                {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
                {"id": "mean", "parameters": [{"name": "data"}]},
            ]})
        else:
            requests_mock.get(backend2 + "/processes", status_code=404, text="nope")
        res = api100.get("/processes").assert_status_code(200).json
        assert res == {"processes": expected, "links": []}

    def test_result_basic_math_basic_auth(self, api100, requests_mock, backend1, backend2):
        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            pg = request.json()["process"]["process_graph"]
            (_, node), = pg.items()
            assert node["process_id"] == "add"
            assert node["result"] is True
            context.headers["Content-Type"] = "application/json"
            return node["arguments"]["x"] + node["arguments"]["y"]

        requests_mock.post(backend1 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 8

    def test_result_basic_math_oidc_auth(self, api100, requests_mock, backend1, backend2):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {"sub": "john"}

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer oidc/egi/funiculifunicula"
            pg = request.json()["process"]["process_graph"]
            (_, node), = pg.items()
            assert node["process_id"] == "add"
            assert node["result"] is True
            context.headers["Content-Type"] = "application/json"
            return node["arguments"]["x"] + node["arguments"]["y"]

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        requests_mock.post(backend1 + "/result", json=post_result)
        api100.set_auth_bearer_token(token="oidc/egi/funiculifunicula")
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 8

    @pytest.mark.parametrize("status_code", [201, 302, 404, 500])
    def test_result_basic_math_error(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.post(backend1 + "/result", status_code=status_code, text="nope")
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request)
        res.assert_error(500, "Internal", message="Failed to process synchronously on backend b1")

    @pytest.mark.parametrize(["chunk_size"], [(16,), (128,)])
    def test_result_large_response_streaming(self, config, chunk_size, requests_mock, backend1, backend2):
        config.streaming_chunk_size = chunk_size
        api100 = get_api100(get_flask_app(config))

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/octet-stream"
            return bytes(b % 256 for b in range(1000))

        requests_mock.post(backend1 + "/result", content=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"large": {"process_id": "large", "arguments": {}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)

        assert res.response.is_streamed
        chunks = res.response.iter_encoded()
        first_chunk = next(chunks)
        assert len(first_chunk) == chunk_size
        assert first_chunk.startswith(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
        assert len(next(chunks)) == chunk_size
        assert len(res.data) == 1000 - 2 * chunk_size

    @pytest.mark.parametrize(["cid", "call_counts"], [
        ("S1", (1, 0)),
        ("S10", (1, 0)),
        ("S2", (0, 1)),
        ("S20", (0, 1)),
    ])
    def test_result_backend_by_collection(self, api100, requests_mock, backend1, backend2, cid, call_counts):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S10"}, ]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S20"}, ]})

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": cid}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 123
        assert (b1_mock.call_count, b2_mock.call_count) == call_counts

    def test_result_backend_by_collection_collection_not_found(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S3"}, "result": True}}
        res = api100.post("/result", json={"process": {"process_graph": pg}})
        res.assert_error(404, "CollectionNotFound", "Collection 'S3' does not exist")

    @pytest.mark.parametrize("pg", [
        {"lc": {}},
        {"lc": {"foo": "bar"}},
        {"lc": {"process_id": "load_collection"}},
        {"lc": {"process_id": "load_collection", "arguments": {}}},
    ])
    def test_result_backend_by_collection_invalid_pg(self, api100, requests_mock, backend1, backend2, pg):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/result", json={"process": {"process_graph": pg}})
        res.assert_error(400, "ProcessGraphInvalid")

    @pytest.mark.parametrize(["user_selected_backend", "expected_response", "expected_call_counts"], [
        ("b1", (200, None), (1, 0)),
        ("b2", (200, None), (0, 1)),
        ("b3", (400, "BackendLookupFailure"), (0, 0)),
    ])
    def test_load_collection_from_user_selected_backend(
            self, api100, backend1, backend2, requests_mock,
            user_selected_backend, expected_response, expected_call_counts
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2"})

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "S2",
                "properties": {AggregatorCollectionCatalog.STAC_PROPERTY_PROVIDER_BACKEND: {"process_graph": {
                    "eq": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": user_selected_backend},
                        "result": True
                    }
                }}}
            },
            "result": True
        }}
        request = {"process": {"process_graph": pg}}
        response = api100.post("/result", json=request)

        expected_status, expected_error_code = expected_response
        if expected_status < 400:
            response.assert_status_code(expected_status)
        else:
            response.assert_error(status_code=expected_status, error_code=expected_error_code)

        assert (b1_mock.call_count, b2_mock.call_count) == expected_call_counts

    def test_load_result_job_id_parsing_basic(self, api100, requests_mock, backend1, backend2):
        """https://github.com/Open-EO/openeo-aggregator/issues/19"""

        def b1_post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg == {"load": {"process_id": "load_result", "arguments": {"id": "b6tch-j08"}, "result": True}}
            context.headers["Content-Type"] = "application/json"
            return 111

        def b2_post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg == {"load": {"process_id": "load_result", "arguments": {"id": "897c5-108"}, "result": True}}
            context.headers["Content-Type"] = "application/json"
            return 222

        b1_mock = requests_mock.post(backend1 + "/result", json=b1_post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=b2_post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b1-b6tch-j08"}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 111
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 0)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b2-897c5-108"}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 222
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 1)

    @pytest.mark.parametrize(["job_id", "s2_backend", "expected_success"], [
        ("b1-b6tch-j08", 1, True),
        ("b2-b6tch-j08", 1, False),
        ("b1-b6tch-j08", 2, False),
        ("b2-b6tch-j08", 2, True),
    ])
    def test_load_result_job_id_parsing_with_load_collection(
            self, api100, requests_mock, backend1, backend2, job_id, s2_backend, expected_success
    ):
        """https://github.com/Open-EO/openeo-aggregator/issues/19"""

        backend_root = {1: backend1, 2: backend2}[s2_backend]
        requests_mock.get(backend_root + "/collections", json={"collections": [{"id": "S2"}]})

        def post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg["lr"]["arguments"]["id"] == "b6tch-j08"
            context.headers["Content-Type"] = "application/json"

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)

        pg = {
            "lr": {"process_id": "load_result", "arguments": {"id": job_id}},
            "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "merge": {"process_id": "merge_cubes", "arguments": {
                "cube1": {"from_node": "lr"},
                "cube2": {"from_node": "lc"}
            }}
        }
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        request = {"process": {"process_graph": pg}}
        if expected_success:
            api100.post("/result", json=request).assert_status_code(200)
            assert (b1_mock.call_count, b2_mock.call_count) == {1: (1, 0), 2: (0, 1)}[s2_backend]
        else:
            api100.post("/result", json=request).assert_error(400, "BackendLookupFailure")
            assert (b1_mock.call_count, b2_mock.call_count) == (0, 0)


class TestBatchJobs:

    def test_list_jobs_no_auth(self, api100):
        api100.get("/jobs").assert_error(401, "AuthenticationRequired")

    def test_list_jobs(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
        ]})
        requests_mock.get(backend2 + "/jobs", json={"jobs": [
            {"id": "job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
        ]})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res["jobs"] == [
            {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
        ]

    @pytest.mark.parametrize("b2_oidc_pid", ["egi", "aho"])
    def test_list_jobs_oidc_pid_mapping(self, config, requests_mock, backend1, backend2, b2_oidc_pid):
        # Override /credentials/oidc of backend2 before building flask app and ApiTester
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": b2_oidc_pid, "issuer": "https://egi.test", "title": "EGI"}
        ]})
        api100 = get_api100(get_flask_app(config))

        # OIDC setup
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer t0k3n"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        def b1_get_jobs(request, context):
            assert request.headers["Authorization"] == "Bearer oidc/egi/t0k3n"
            return {"jobs": [
                {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            ]}

        def b2_get_jobs(request, context):
            assert request.headers["Authorization"] == f"Bearer oidc/{b2_oidc_pid}/t0k3n"
            return {"jobs": [
                {"id": "job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
            ]}

        requests_mock.get(backend1 + "/jobs", json=b1_get_jobs)
        requests_mock.get(backend2 + "/jobs", json=b2_get_jobs)

        api100.set_auth_bearer_token(token="oidc/egi/t0k3n")
        res = api100.get("/jobs").assert_status_code(200).json
        assert res["jobs"] == [
            {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
        ]

    @pytest.mark.parametrize("status_code", [204, 303, 404, 500])
    def test_list_jobs_failing_backend(self, api100, requests_mock, backend1, backend2, caplog, status_code):
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
        ]})
        requests_mock.get(backend2 + "/jobs", status_code=status_code, json={"code": "nope", "message": "and nope"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res["jobs"] == [
            {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
        ]

        warnings = "\n".join(r.msg for r in caplog.records if r.levelno == logging.WARNING)
        assert "Failed to get job listing from backend 'b2'" in warnings

    def test_create_job(self, api100, requests_mock, backend1):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        def post_jobs(request: requests.Request, context):
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}}).assert_status_code(201)
        assert res.headers["Location"] == "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b"
        assert res.headers["OpenEO-Identifier"] == "b1-th3j0b"

    @pytest.mark.parametrize("body", [
        {"foo": "meh"},
        {"process": "meh"},
    ])
    def test_create_job_pg_missing(self, api100, requests_mock, backend1, body):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json=body)
        res.assert_error(400, "ProcessGraphMissing")

    @pytest.mark.parametrize("body", [
        {"process": {"process_graph": "meh"}},
        {"process": {"process_graph": {}}},
        {"process": {"process_graph": {"foo": "meh"}}},
        {"process": {"process_graph": {"foo": {"bar": "meh"}}}},
        {"process": {"process_graph": {"foo": {"process_id": "meh"}}}},
    ])
    def test_create_job_pg_invalid(self, api100, requests_mock, backend1, body):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.post(
            backend1 + "/jobs",
            status_code=ProcessGraphInvalidException.status_code,
            json=ProcessGraphInvalidException().to_dict(),
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json=body)
        res.assert_error(400, "ProcessGraphInvalid")

    @pytest.mark.parametrize("status_code", [200, 201, 500])
    def test_create_job_backend_failure(self, api100, requests_mock, backend1, status_code):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        def post_jobs(request: requests.Request, context):
            # Go wrong here: missing headers or unexpected status code
            context.status_code = status_code

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}})
        res.assert_error(500, "Internal", message="Failed to create job on backend 'b1'")

    def test_get_job_metadata(self, api100, requests_mock, backend1):
        requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "process": {"process_graph": {
                "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}
            }},
            "status": "running", "progress": 42, "created": "2017-01-01T09:32:12Z",
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b").assert_status_code(200)
        assert res.json == {
            "id": "b1-th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "process": {"process_graph": {
                "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}
            }},
            "status": "running", "progress": 42, "created": "2017-01-01T09:32:12Z",
        }

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_job_metadata_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_job_metadata_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_start_job(self, api100, requests_mock, backend1):
        m = requests_mock.post(backend1 + "/jobs/th3j0b/results", status_code=202)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.post("/jobs/b1-th3j0b/results").assert_status_code(202)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_start_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.post(
            backend1 + f"/jobs/{job_id}/results",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_start_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_cancel_job(self, api100, requests_mock, backend1):
        m = requests_mock.delete(backend1 + "/jobs/th3j0b/results", status_code=204)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.delete("/jobs/b1-th3j0b/results").assert_status_code(204)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_cancel_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.delete(
            backend1 + f"/jobs/{job_id}/results",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_cancel_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_delete_job(self, api100, requests_mock, backend1):
        m = requests_mock.delete(backend1 + "/jobs/th3j0b", status_code=204)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.delete("/jobs/b1-th3j0b").assert_status_code(204)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_delete_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.delete(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete(f"/jobs/b1-{job_id}")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_delete_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete("/jobs/nope-and-nope")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_get_results(self, api100, requests_mock, backend1):
        m1 = requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "status": "finished", "progress": 100, "created": "2017-01-01T09:32:12Z",
        })
        m2 = requests_mock.get(backend1 + "/jobs/th3j0b/results", status_code=200, json={
            "assets": {
                "r1.tiff": {"href": "https//res.b1.test/123/r1.tiff", "title": "Result 1"}
            }
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results").assert_status_code(200).json
        assert m1.call_count == 1
        assert m2.call_count == 1
        assert res["assets"] == {
            "r1.tiff": {
                "href": "https//res.b1.test/123/r1.tiff",
                "title": "Result 1",
                "roles": ["data"],
                "file:nodata": [None],
                "type": "application/octet-stream",
            }
        }
        assert res["id"] == "b1-th3j0b"
        assert res["type"] == "Feature"
        assert_dict_subset(
            {"title": "The job", "created": "2017-01-01T09:32:12Z", "description": "Just doing my job."},
            res["properties"]
        )

    @pytest.mark.parametrize("job_status", ["created", "running", "canceled", "error"])
    def test_get_results_not_finished(self, api100, requests_mock, backend1, job_status):
        requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b", "status": job_status, "created": "2017-01-01T09:32:12Z",
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results")
        res.assert_error(JobNotFinishedException.status_code, "JobNotFinished")

    def test_get_results_finished_unreliable(self, api100, requests_mock, backend1):
        """Edge case: job status is 'finished', but results still return with 'JobNotFinished'."""
        m1 = requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b", "status": "finished", "created": "2017-01-01T09:32:12Z",
        })
        m2 = requests_mock.get(
            backend1 + "/jobs/th3j0b/results",
            status_code=JobNotFinishedException.status_code, json=JobNotFinishedException().to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results")
        res.assert_error(JobNotFinishedException.status_code, "JobNotFinished")
        assert m1.call_count == 1
        assert m2.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_results_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_results_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_get_logs(self, api100, requests_mock, backend1):
        def get_logs(request, context):
            offset = request.qs.get("offset", ["_"])[0]
            return {"logs": [
                {"id": offset + "1", "level": "info", "message": "hello"},
                {"id": offset + "11", "level": "info", "message": "hello"},
            ]}

        requests_mock.get(backend1 + "/jobs/th3j0b/logs", status_code=200, json=get_logs)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/logs").assert_status_code(200).json
        assert res == {
            "logs": [
                {"id": "_1", "level": "info", "message": "hello"},
                {"id": "_11", "level": "info", "message": "hello"},
            ],
            "links": []
        }

        res = api100.get("/jobs/b1-th3j0b/logs?offset=3").assert_status_code(200).json
        assert res == {
            "logs": [
                {"id": "31", "level": "info", "message": "hello"},
                {"id": "311", "level": "info", "message": "hello"},
            ],
            "links": []
        }

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_logs_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}/logs",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}/logs")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_logs_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope/logs")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")


class TestResilience:

    @pytest.fixture
    def broken_backend2(
            self, backend1, requests_mock, base_config
    ) -> Tuple[str, AggregatorConfig, 'requests_mock.adapter._Matcher']:
        """Fixture to quickly set up a config with broken backend2"""
        backend2 = "https://b2.test/v1"
        # TODO: return 500 on all requests?
        root_mock = requests_mock.get(backend2 + "/", status_code=500)

        config = base_config.copy()
        config.aggregator_backends = {"b1": backend1, "b2": backend2}
        return backend2, config, root_mock

    def test_startup_during_backend_downtime(self, backend1, broken_backend2, requests_mock, caplog):
        caplog.set_level(logging.WARNING)

        # Initial backend setup with broken backend2
        requests_mock.get(backend1 + "/health", text="OK")
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        assert "Failed to create backend 'b2' connection" in caplog.text
        assert b2_root.call_count == 1

        api100.get("/").assert_status_code(200)

        resp = api100.get("/health").assert_status_code(200)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": 200,
        }

    def test_startup_during_backend_downtime_and_recover(self, backend1, broken_backend2, requests_mock):
        # Set up fake clock
        MultiBackendConnection._clock = itertools.count(1).__next__

        # Initial backend setup with broken backend2
        requests_mock.get(backend1 + "/health", text="OK")
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
            "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
        }

        # Backend 2 is up again, but cached is still active
        requests_mock.get(backend2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
        ]})
        requests_mock.get(backend2 + "/health", text="ok again")
        assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
            "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
        }

        # Wait a bit so that cache is flushed
        MultiBackendConnection._clock = itertools.count(1000).__next__
        assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
            "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
            "b2": {"status_code": 200, "text": "ok again", "response_time": pytest.approx(0.1, abs=0.1)},
        }

    @pytest.mark.parametrize("b2_oidc_provider_id", ["egi", "aho"])
    def test_oidc_mapping_after_recover(self, backend1, broken_backend2, requests_mock, b2_oidc_provider_id):
        # Set up fake clock
        MultiBackendConnection._clock = itertools.count(1).__next__

        # Initial backend setup with broken backend2
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        # OIDC setup
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer t0k3n"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        # Job listings: backend1 works, backend2 is down
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]})
        requests_mock.get(backend2 + "/jobs", status_code=500, text="nope")

        api100.set_auth_bearer_token(token="oidc/egi/t0k3n")
        jobs = api100.get("/jobs").assert_status_code(200).json
        assert jobs["jobs"] == [
            {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]

        # Backend2 is up again (but still cached as down)
        requests_mock.get(backend2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": b2_oidc_provider_id, "issuer": "https://egi.test", "title": "EGI"}
        ]})

        def get_jobs(request, context):
            assert request.headers["Authorization"] == f"Bearer oidc/{b2_oidc_provider_id}/t0k3n"
            return {"jobs": [
                {"id": "j0b2", "status": "running", "created": "2021-02-22T22:22:22Z"}
            ]}

        requests_mock.get(backend2 + "/jobs", json=get_jobs)

        jobs = api100.get("/jobs").assert_status_code(200).json
        assert jobs["jobs"] == [
            {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]

        # Skip time so that connection cache is cleared
        MultiBackendConnection._clock = itertools.count(1000).__next__
        jobs = api100.get("/jobs").assert_status_code(200).json
        assert jobs["jobs"] == [
            {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"},
            {"id": "b2-j0b2", "status": "running", "created": "2021-02-22T22:22:22Z"},
        ]
