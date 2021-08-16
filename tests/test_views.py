import pytest
import requests

from openeo_aggregator.app import create_app
from openeo_driver.errors import JobNotFoundException, ProcessGraphMissingException, JobNotFinishedException
from openeo_driver.testing import ApiTester, TEST_USER_AUTH_HEADER, TEST_USER, TEST_USER_BEARER_TOKEN
from .conftest import assert_dict_subset


@pytest.fixture
def api100(flask_app) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=flask_app.test_client())


class TestGeneral:
    def test_capabilities(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        assert capabilities["api_version"] == "1.0.0"
        endpoints = capabilities["endpoints"]
        assert {"methods": ["GET"], "path": "/collections"} in endpoints
        assert {"methods": ["GET"], "path": "/collections/{collection_id}"} in endpoints
        assert {"methods": ["GET"], "path": "/processes"} in endpoints

    def test_info(self, flask_app):
        api100 = ApiTester(api_version="1.0.0", client=flask_app.test_client(), url_root="/")
        res = api100.get("_info").assert_status_code(200)
        assert res.json == {
            "backends": [{"id": "b1", "root_url": "https://b1.test/v1"}, {"id": "b2", "root_url": "https://b2.test/v1"}]
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
        assert set(c["id"] for c in res["collections"]) == {"S1", "S3"}


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
        api100 = ApiTester(api_version="1.0.0", client=create_app(config).test_client())

        res = api100.get("/credentials/oidc").assert_status_code(200).json
        assert res == {"providers": [
            {"id": "y", "issuer": "https://y.test", "title": "YY", "scopes": ["openid"]}
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
            "processes": [{"id": "mean", "parameters": [{"name": "data"}]}],
            "links": [],
        }

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

    @pytest.mark.parametrize(["chunk_size"], [(16,), (128,)])
    def test_result_large_response_streaming(self, config, chunk_size, requests_mock, backend1, backend2):
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
        res.assert_error(400, "ProcessGraphMissing")


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
        {"process": {"process_graph": "meh"}},
        {"process": {"process_graph": {}}},
        {"process": {"process_graph": {"foo": "meh"}}},
    ])
    def test_create_job_invalid(self, api100, requests_mock, backend1, body):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.post(
            backend1 + "/jobs",
            status_code=ProcessGraphMissingException.status_code, json=ProcessGraphMissingException().to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json=body)
        res.assert_error(400, "ProcessGraphMissing")

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
