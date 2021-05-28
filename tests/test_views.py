import pytest
import requests

from openeo_aggregator.app import create_app
from openeo_driver.testing import ApiTester, TEST_USER_AUTH_HEADER, TEST_USER, TEST_USER_BEARER_TOKEN


@pytest.fixture
def api100(flask_app) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=flask_app.test_client())


def test_capabilities(api100):
    res = api100.get("/").assert_status_code(200)
    capabilities = res.json
    assert capabilities["api_version"] == "1.0.0"
    endpoints = capabilities["endpoints"]
    assert {"methods": ["GET"], "path": "/collections"} in endpoints
    assert {"methods": ["GET"], "path": "/collections/{collection_id}"} in endpoints
    assert {"methods": ["GET"], "path": "/processes"} in endpoints


def test_collections_basic(api100, requests_mock, backend1, backend2):
    requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
    requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
    res = api100.get("/collections").assert_status_code(200).json
    assert set(c["id"] for c in res["collections"]) == {"S1", "S2", "S3"}


def test_collections_duplicate(api100, requests_mock, backend1, backend2):
    requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
    requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S3"}]})
    res = api100.get("/collections").assert_status_code(200).json
    assert set(c["id"] for c in res["collections"]) == {"S1", "S3"}


def test_processes_basic(api100, requests_mock, backend1, backend2):
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
        "processes": [{"id": "mean", "parameters": [{"name": "data"}]}],
        "links": [],
    }


def test_credentials_oidc_default(api100, backend1, backend2):
    res = api100.get("/credentials/oidc").assert_status_code(200).json
    assert res == {"providers": [
        {"id": "egi", "issuer": "https://egi.test", "title": "EGI", "scopes": ["openid"]}
    ]}


def test_credentials_oidc_intersection(requests_mock, config, backend1, backend2):
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
    app = create_app(config)
    api100 = ApiTester(api_version="1.0.0", client=app.test_client())

    res = api100.get("/credentials/oidc").assert_status_code(200).json
    assert res == {"providers": [
        {"id": "y", "issuer": "https://y.test", "title": "YY", "scopes": ["openid"]}
    ]}


def test_me_unauthorized(api100):
    api100.get("/me").assert_error(401, "AuthenticationRequired")


def test_me_basic_auth_invalid(api100):
    headers = {"Authorization": "Bearer " + "basic//foobar"}
    api100.get("/me", headers=headers).assert_error(403, "TokenInvalid")


def test_me_basic_auth(api100):
    headers = TEST_USER_AUTH_HEADER
    res = api100.get("/me", headers=headers).assert_status_code(200)
    assert res.json["user_id"] == TEST_USER


def test_result_basic_math(api100, requests_mock, backend1, backend2):
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
