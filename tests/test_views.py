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
    api100 = ApiTester(api_version="1.0.0", client=create_app(config).test_client())

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


def test_result_basic_math_oidc_auth(api100, requests_mock, backend1, backend2):
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


@pytest.mark.parametrize(["chunk_size"], [(16,), (128,)])
def test_result_large_response_streaming(config, chunk_size, requests_mock, backend1, backend2):
    config.streaming_chunk_size = chunk_size
    api100 = ApiTester(api_version="1.0.0", client=create_app(config).test_client())

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
def test_result_backend_by_collection(api100, requests_mock, backend1, backend2, cid, call_counts):
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
