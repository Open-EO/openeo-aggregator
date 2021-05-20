import pytest

from openeo_driver.testing import ApiTester


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client)


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
