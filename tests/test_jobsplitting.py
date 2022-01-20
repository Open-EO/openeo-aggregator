from unittest import mock

import datetime
import itertools
import time

import flask
import requests
import kazoo
import kazoo.exceptions
import pytest

from openeo_aggregator.jobsplitting import PartitionedJob, SubJob, ZooKeeperPartitionedJobDB, PartitionedJobTracker
from openeo_aggregator.testing import DummyKazooClient, str_starts_with
from openeo_driver.testing import TEST_USER_BEARER_TOKEN

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
    def ids(prefix="", max_attemtps=5):
        for i in range(start, start + max_attemtps):
            prefix = prefix.format(date="20220117-174800")
            yield f"{prefix}{i}"

    return mock.patch("openeo_aggregator.jobsplitting.generate_id_candidates", new=ids)


def now_approx(abs=10):
    """Pytest checker for whether timestamp is approximately 'now' (within some tolerance)."""
    return pytest.approx(time.time(), abs=abs)


class TestZooKeeperPartitionedJobDB:

    def test_insert_basic(self, pjob, zk_client, zk_db):
        with mock_generate_id_candidates():
            pjob_id = zk_db.insert(pjob)
        assert pjob_id == "pj-20220117-174800-5"

        data = zk_client.get_data_deserialized(drop_empty=True)
        assert data == {
            "/t/pj-20220117-174800-5": {
                "created": now_approx(),
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
            },
            "/t/pj-20220117-174800-5/sjobs/0000/status": {
                "status": "inserted"
            },
            "/t/pj-20220117-174800-5/sjobs/0001": {
                "process_graph": PG23,
                "backend_id": "b2",
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
            "created": now_approx(),
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
            },
            "0001": {
                "process_graph": PG23,
                "backend_id": "b2",
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
        assert status == {"status": "running", "message": "goin' on", "timestamp": now_approx()}

    def test_set_get_sjob_status(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")

        with mock_generate_id_candidates():
            zk_db.insert(pjob)

        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")
        assert status == {"status": "inserted"}

        zk_db.set_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000", status="running", message="goin' on")
        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800-5", sjob_id="0000")
        assert status == {"status": "running", "message": "goin' on", "timestamp": now_approx()}


class TestPartitionedJobTracker:

    @pytest.fixture
    def flask_request(self) -> flask.Request:
        return flask.Request(environ={"HTTP_AUTHORIZATION": "Bearer oidc/egi/l3tm31n"})

    def test_submit(self, pjob, zk_client, zk_db, zk_tracker):
        pjob_id = zk_tracker.submit(pjob)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {"status": "inserted"}
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {"status": "inserted"}

    def test_sync_error_no_http(self, pjob, zk_client, zk_db, zk_tracker, flask_request):
        """Simple failure use case: no working mock requests to backends"""
        pjob_id = zk_tracker.submit(pjob)
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": "subjob stats: {'error': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "error",
                "message": str_starts_with("Failed to create subjob: No mock address:"),
                "timestamp": now_approx(),
            }

    @staticmethod
    def _post_jobs_handler(backend: str, job_id: str):
        """Create requests_mock handler for `POST /jobs`"""

        def post_jobs(request: requests.Request, context):
            context.headers["Location"] = f"{backend}/jobs/{job_id}"
            context.headers["OpenEO-Identifier"] = job_id
            context.status_code = 201

        return post_jobs

    def test_sync_basic(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2):
        pjob_id = zk_tracker.submit(pjob)

        # First sync
        requests_mock.post(backend1 + "/jobs", text=self._post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs", text=self._post_jobs_handler(backend2, "2jo8"))
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": "subjob stats: {'running': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "running",
                "message": "started",
                "timestamp": now_approx(),
            }

        # Sync #2 (both still running)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "running"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": "subjob stats: {'running': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "running",
                "message": "running",
                "timestamp": now_approx(),
            }

        # Sync #3 (one finished)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "finished"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": "subjob stats: {'running': 1, 'finished': 1}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "running",
            "timestamp": now_approx(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "finished",
            "message": None,
            "timestamp": now_approx(),
        }

        # Sync #4 (both finished)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "finished"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "finished"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "finished",
            "message": "subjob stats: {'finished': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "finished",
                "message": None,
                "timestamp": now_approx(),
            }

    def test_sync_with_error(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1,
                             backend2):
        pjob_id = zk_tracker.submit(pjob)

        # First sync
        requests_mock.post(backend1 + "/jobs", text=self._post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs", text=self._post_jobs_handler(backend2, "2jo8"))
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": "subjob stats: {'running': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "running",
                "message": "started",
                "timestamp": now_approx(),
            }

        # Sync #2
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "error"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": "subjob stats: {'running': 1, 'error': 1}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "running",
            "timestamp": now_approx(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "error",
            "timestamp": now_approx(),
        }

        # Sync #3: another error (note invalid status too)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "3rr0r"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "error"})
        zk_tracker.sync(pjob_id=pjob_id, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": "subjob stats: {'error': 2}",
            "timestamp": now_approx(),
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "error",
            "message": "3rr0r",
            "timestamp": now_approx(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "error",
            "timestamp": now_approx(),
        }


class TestBatchJobSplitting:

    def test_create_job_basic(self, api100, backend1):
        # TODO: put this in helper, fixture, or setup procedure?
        ZooKeeperPartitionedJobDB._clock = itertools.count(
            datetime.datetime(2022, 1, 19, tzinfo=datetime.timezone.utc).timestamp()).__next__

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
            "created": "2022-01-19T00:00:00Z",
        }
