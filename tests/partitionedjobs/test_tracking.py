import flask
import pytest
import requests

from openeo_aggregator.partitionedjobs.tracking import PartitionedJobTracker, PartitionedJobConnection
from openeo_aggregator.testing import approx_now, approx_str_prefix, approx_str_contains
from openeo_driver.errors import JobNotFoundException
from openeo_driver.testing import DictSubSet, TEST_USER
from .conftest import OTHER_TEST_USER


@pytest.fixture
def zk_tracker(zk_db, multi_backend_connection) -> PartitionedJobTracker:
    return PartitionedJobTracker(db=zk_db, backends=multi_backend_connection)


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

        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)

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
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)

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
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)

        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)
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

    def test_start_wrong_user(
            self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2
    ):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })

        with pytest.raises(JobNotFoundException):
            zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=OTHER_TEST_USER, flask_request=flask_request)

    def test_sync_basic(self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (both still running)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "running"})
        zk_tracker.sync(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)

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
        zk_tracker.sync(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)

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
        zk_tracker.sync(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)

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
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (with error)
        requests_mock.get(backend1 + "/jobs/1j0b", json={"id": "1j0b", "status": "running"})
        requests_mock.get(backend2 + "/jobs/2jo8", json={"id": "2j08", "status": "error"})
        zk_tracker.sync(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)

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
        zk_tracker.sync(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)

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

    def test_sync_wrong_user(
            self, pjob, zk_client, zk_db, zk_tracker, flask_request, requests_mock, backend1, backend2
    ):
        # Create
        requests_mock.post(backend1 + "/jobs", text=_post_jobs_handler(backend1, "1j0b"))
        requests_mock.post(backend2 + "/jobs", text=_post_jobs_handler(backend2, "2jo8"))
        pjob_id = zk_tracker.create(pjob=pjob, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        requests_mock.post(backend1 + "/jobs/1j0b/results", status_code=202)
        requests_mock.post(backend2 + "/jobs/2jo8/results", status_code=202)
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=TEST_USER, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (both still running)
        with pytest.raises(JobNotFoundException):
            zk_tracker.sync(pjob_id=pjob_id, user_id=OTHER_TEST_USER, flask_request=flask_request)


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
