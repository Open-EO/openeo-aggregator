import datetime
from unittest import mock

import flask
import requests
import kazoo
import kazoo.exceptions
import pytest

from openeo.util import rfc3339
from openeo_aggregator.jobsplitting import PartitionedJob, SubJob, ZooKeeperPartitionedJobDB, PartitionedJobTracker, \
    PartitionedJobConnection
from openeo_aggregator.testing import clock_mock, approx_now, approx_str_prefix, approx_str_contains
from openeo_driver.testing import TEST_USER_BEARER_TOKEN, DictSubSet

PG12 = {
    "add": {"process_id": "add", "arguments": {"X": 1, "y": 2}, "result": True}
}
PG23 = {
    "add": {"process_id": "add", "arguments": {"X": 2, "y": 3}, "result": True}
}
PG35 = {
    "add": {"process_id": "add", "arguments": {"X": 3, "y": 5}, "result": True}
}
P35 = {"process_graph": PG35}


@pytest.fixture
def pjob():
    return PartitionedJob(
        process=P35,
        metadata={},
        job_options={},
        subjobs=[
            SubJob(process_graph=PG12, backend_id="b1"),
            SubJob(process_graph=PG23, backend_id="b2"),
        ]
    )


@pytest.fixture
def zk_db(zk_client) -> ZooKeeperPartitionedJobDB:
    return ZooKeeperPartitionedJobDB(client=zk_client, prefix="/t")


@pytest.fixture
def zk_tracker(zk_db, multi_backend_connection) -> PartitionedJobTracker:
    return PartitionedJobTracker(db=zk_db, backends=multi_backend_connection)


def mock_generate_id_candidates(start=5):
    # TODO can this mock be avoided (mock os.random, or make sure to seed RNG)?
    def ids(prefix="", max_attemtps=5):
        for i in range(start, start + max_attemtps):
            prefix = prefix.format(date="20220117-174800")
            yield f"{prefix}{i}"

    return mock.patch("openeo_aggregator.jobsplitting.generate_id_candidates", new=ids)


class TestZooKeeperPartitionedJobDB:

    def test_insert_basic(self, pjob, zk_client, zk_db):
        with mock_generate_id_candidates():
            pjob_id = zk_db.insert(pjob)
        assert pjob_id == "pj-20220117-174800-5"

        data = zk_client.get_data_deserialized(drop_empty=True)
        assert data == {
            "/t/pj-20220117-174800-5": {
                "created": approx_now(),
                "user": "TODO",
                "process": P35,
                "metadata": {},
                "job_options": {},
            },
            "/t/pj-20220117-174800-5/status": {
                "status": "inserted",
            },
            "/t/pj-20220117-174800-5/sjobs/0000": {
                "process_graph": PG12,
                "backend_id": "b1",
                "title": "Partitioned job pj-20220117-174800-5 part 0000 (1/2)"
            },
            "/t/pj-20220117-174800-5/sjobs/0000/status": {
                "status": "inserted"
            },
            "/t/pj-20220117-174800-5/sjobs/0001": {
                "process_graph": PG23,
                "backend_id": "b2",
                "title": "Partitioned job pj-20220117-174800-5 part 0001 (2/2)"
            },
            "/t/pj-20220117-174800-5/sjobs/0001/status": {
                "status": "inserted"
            },
        }

    def test_get_pjob_metadata(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_pjob_metadata("pj-20220117-174800-5")

        with mock_generate_id_candidates():
            zk_db.insert(pjob)

        assert zk_db.get_pjob_metadata("pj-20220117-174800-5") == {
            "created": approx_now(),
            "user": "TODO",
            "process": P35,
            "metadata": {},
            "job_options": {},
        }

    def test_list_subjobs(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.list_subjobs("pj-20220117-174800-5")

        with mock_generate_id_candidates():
            zk_db.insert(pjob)

        assert zk_db.list_subjobs("pj-20220117-174800-5") == {
            "0000": {
                "process_graph": PG12,
                "backend_id": "b1",
                "title": "Partitioned job pj-20220117-174800-5 part 0000 (1/2)"
            },
            "0001": {
                "process_graph": PG23,
                "backend_id": "b2",
                "title": "Partitioned job pj-20220117-174800-5 part 0001 (2/2)"
            },
        }

    def test_set_get_backend_job_id(self, pjob, zk_db):
        with mock_generate_id_candidates():
            pjob_id = zk_db.insert(pjob)

        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_backend_job_id(pjob_id=pjob_id, sjob_id="0000")

        zk_db.set_backend_job_id(pjob_id=pjob_id, sjob_id="0000", job_id="b1-job-123")

        assert zk_db.get_backend_job_id(pjob_id=pjob_id, sjob_id="0000") == "b1-job-123"

    def test_set_get_pjob_status(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_pjob_status(pjob_id="pj-20220117-174800-5")

        with mock_generate_id_candidates():
            zk_db.insert(pjob)

        status = zk_db.get_pjob_status(pjob_id="pj-20220117-174800-5")
        assert status == {"status": "inserted"}

        zk_db.set_pjob_status(pjob_id="pj-20220117-174800-5", status="running", message="goin' on")
        status = zk_db.get_pjob_status(pjob_id="pj-20220117-174800-5")
        assert status == {"status": "running", "message": "goin' on", "timestamp": approx_now()}

    def test_set_get_sjob_status(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")

        with mock_generate_id_candidates():
            zk_db.insert(pjob)

        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")
        assert status == {"status": "inserted"}

        zk_db.set_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000", status="running", message="goin' on")
        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")
        assert status == {"status": "running", "message": "goin' on", "timestamp": approx_now()}


def _post_jobs_handler(backend: str, job_id: str):
    """Create requests_mock handler for `POST /jobs`"""

    def post_jobs(request: requests.Request, context):
        context.headers["Location"] = f"{backend}/jobs/{job_id}"
        context.headers["OpenEO-Identifier"] = job_id
        context.status_code = 201

    return post_jobs


class TestPartitionedJobTracker:

    @pytest.fixture
    def flask_request(self) -> flask.Request:
        return flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/egi/l3tm31n"})

    def test_create(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2):
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))

        pjob_id = zk_tracker.create(pjob, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet({
                "status": "created",
                "message": "created",
            })

    def test_create_error_no_http(self, pjob, zk_client, zk_db, zk_tracker, flask_request):
        """Simple failure use case: no working mock requests to backends"""
        pjob_id = zk_tracker.create(pjob, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": approx_str_contains("{'error': 2}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "error",
                "message": approx_str_prefix("Create failed: No mock address:"),
                "timestamp": approx_now(),
            }

    def test_start(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)

        zk_tracker.start_sjobs(pjob_id=pjob_id, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "running",
            "message": approx_str_contains("{'running': 2}"),
        })
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet({
                "status": "running",
                "message": "started",
            })

    def test_sync_basic(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.start_sjobs(pjob_id=pjob_id, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (both still running)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "running"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 2}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "running",
                "message": "running",
                "timestamp": approx_now(),
            }

        # Sync (one finished)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "finished"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 1, 'finished': 1}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "running",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "finished",
            "message": "finished",
            "timestamp": approx_now(),
        }

        # Sync (both finished)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "finished"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "finished"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "finished",
            "message": approx_str_contains("{'finished': 2}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "finished",
                "message": "finished",
                "timestamp": approx_now(),
            }

    def test_sync_with_error(
            self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1,
            backend2
    ):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.start_sjobs(pjob_id=pjob_id, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (with error)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "error"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 1, 'error': 1}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "running",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "error",
            "timestamp": approx_now(),
        }

        # Sync (another error, and note invalid status too)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "3rr0r"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "error"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": approx_str_contains("{'error': 2}"),
            "timestamp": approx_now(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "error",
            "message": "3rr0r",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "error",
            "timestamp": approx_now(),
        }


class TestBatchJobSplitting:
    now_rfc3339: str = "2022-01-19T12:34:56Z"
    now_epoch: float = rfc3339.parse_datetime(now_rfc3339).replace(tzinfo=datetime.timezone.utc).timestamp()

    @clock_mock(now_rfc3339)
    def test_create_job_basic(self, api100, backend1, zk_client, requests_mock):
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        with mock_generate_id_candidates():
            res = api100.post("/jobs", json={
                "title": "3+5",
                "description": "Addition of 3 and 5",
                "process": P35,
                "plan": "free",
                "job_options": {"_jobsplitting": True}
            }).assert_status_code(201)

        expected_job_id = "agg-pj-20220117-174800-5"
        assert res.headers["Location"] == f"http://oeoa.test/openeo/1.0.0/jobs/{expected_job_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == {
            "id": expected_job_id,
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "status": "created",
            "created": self.now_rfc3339,
        }

        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        assert zk_data["/t/pj/pj-20220117-174800-5"] == {
            "user": "TODO",
            "created": self.now_epoch,
            "process": P35,
            "metadata": {"title": "3+5", "description": "Addition of 3 and 5", "plan": "free"},
            "job_options": {"_jobsplitting": True},
        }
        assert zk_data["/t/pj/pj-20220117-174800-5/status"] == {
            "status": "created",
            "message": approx_str_contains("{'created': 1}"),
            "timestamp": pytest.approx(self.now_epoch, abs=5)
        }
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000"] == {
            "backend_id": "b1",
            "process_graph": PG35,
            "title": "Partitioned job pj-20220117-174800-5 part 0000 (1/1)"
        }
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/job_id"] == {
            "job_id": "1j0b",
        }
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/status"] == {
            "status": "created",
            "message": "created",
            "timestamp": pytest.approx(self.now_epoch, abs=5)
        }

    @clock_mock(now_rfc3339)
    def test_create_job_failed_backend(self, api100, backend1, zk_client, requests_mock):
        requests_mock.post(backend1 + "/jobs", status_code=500, json={"code": "Internal", "message": "nope"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        with mock_generate_id_candidates():
            res = api100.post("/jobs", json={
                "title": "3+5",
                "description": "Addition of 3 and 5",
                "process": P35,
                "plan": "free",
                "job_options": {"_jobsplitting": True}
            }).assert_status_code(201)

        expected_job_id = "agg-pj-20220117-174800-5"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == {
            "id": expected_job_id,
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "status": "error",
            "created": self.now_rfc3339,
        }

        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        assert zk_data["/t/pj/pj-20220117-174800-5/status"] == DictSubSet({
            "status": "error",
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/status"] == DictSubSet({
            "status": "error",
            "message": "Create failed: [500] Internal: nope",
        })

    @clock_mock(now_rfc3339)
    def test_start_job(self, api100, backend1, zk_client, requests_mock):
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        with mock_generate_id_candidates():
            res = api100.post("/jobs", json={
                "process": P35,
                "job_options": {"_jobsplitting": True}
            }).assert_status_code(201)

        expected_job_id = "agg-pj-20220117-174800-5"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "created"})

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running"})

        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        assert zk_data["/t/pj/pj-20220117-174800-5"] == DictSubSet({
            "created": self.now_epoch,
            "process": P35,
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/status"] == DictSubSet({
            "status": "running",
            "message": approx_str_contains("{'running': 1}"),
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/job_id"] == DictSubSet({
            "job_id": "1j0b",
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/status"] == DictSubSet({
            "status": "running",
            "message": "started",
        })

    @clock_mock(now_rfc3339)
    def test_sync_job(self, api100, backend1, zk_client, requests_mock):
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        with mock_generate_id_candidates():
            res = api100.post("/jobs", json={
                "process": P35,
                "job_options": {"_jobsplitting": True}
            }).assert_status_code(201)

        expected_job_id = "agg-pj-20220117-174800-5"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "created"})

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running"})

        # Status check: still running
        requests_mock.get(backend1 + "/jobs/1j0b", json={"status": "running"})
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running"})

        # Status check: finished
        requests_mock.get(backend1 + "/jobs/1j0b", json={"status": "finished"})
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "finished"})

        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        assert zk_data["/t/pj/pj-20220117-174800-5"] == DictSubSet({
            "created": self.now_epoch,
            "process": P35,
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/status"] == DictSubSet({
            "status": "finished",
            "message": approx_str_contains("{'finished': 1}"),
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/job_id"] == DictSubSet({
            "job_id": "1j0b",
        })
        assert zk_data["/t/pj/pj-20220117-174800-5/sjobs/0000/status"] == DictSubSet({
            "status": "finished",
        })

    @clock_mock(now_rfc3339)
    def test_job_results(self, api100, backend1, zk_client, requests_mock):
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        with mock_generate_id_candidates():
            res = api100.post("/jobs", json={
                "process": P35,
                "job_options": {"_jobsplitting": True}
            }).assert_status_code(201)

        expected_job_id = "agg-pj-20220117-174800-5"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running"})

        # Status check: finished
        requests_mock.get(backend1 + "/jobs/1j0b", json={"status": "finished"})
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "finished"})

        # Get results
        requests_mock.get(backend1 + "/jobs/1j0b/results", json={
            "assets": {
                "preview.png": {"href": backend1 + "/jobs/1j0b/results/preview.png"},
                "res001.tif": {"href": backend1 + "/jobs/1j0b/results/res001.tiff"},
                "res002.tif": {"href": backend1 + "/jobs/1j0b/results/res002.tiff"},
            }
        })

        res = api100.get(f"/jobs/{expected_job_id}/results").assert_status_code(200)
        assert res.json == DictSubSet({
            "id": expected_job_id,
            "assets": {
                "preview.png": DictSubSet({"href": backend1 + "/jobs/1j0b/results/preview.png"}),
                "res001.tif": DictSubSet({"href": backend1 + "/jobs/1j0b/results/res001.tiff"}),
                "res002.tif": DictSubSet({"href": backend1 + "/jobs/1j0b/results/res002.tiff"}),
            }
        })


class TestPartitionedJobConnection:

    def test_authenticated_from_request(self, zk_tracker):
        con = PartitionedJobConnection(partitioned_job_tracker=zk_tracker)
        assert con._flask_request is None
        with con.authenticated_from_request(request=flask.Request(environ={"PATH_INFO": "foo"})):
            assert con._flask_request.path == "/foo"
        assert con._flask_request is None

    def test_double_auth(self, zk_tracker):
        con = PartitionedJobConnection(partitioned_job_tracker=zk_tracker)
        assert con._flask_request is None
        with con.authenticated_from_request(request=flask.Request(environ={"PATH_INFO": "foo"})):
            assert con._flask_request.path == "/foo"
            with pytest.raises(RuntimeError, match="Reentering authenticated_from_request"):
                with con.authenticated_from_request(request=flask.Request(environ={"PATH_INFO": "bar"})):
                    pass
            assert con._flask_request.path == "/foo"
        assert con._flask_request is None
