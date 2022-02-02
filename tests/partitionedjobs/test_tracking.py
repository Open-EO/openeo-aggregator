import flask
import pytest

from openeo_aggregator.partitionedjobs.tracking import PartitionedJobTracker, PartitionedJobConnection
from openeo_aggregator.testing import approx_now, approx_str_prefix, approx_str_contains
from openeo_driver.errors import JobNotFoundException
from openeo_driver.testing import DictSubSet
from .conftest import P12, P23, DummyBackend


@pytest.fixture
def zk_tracker(zk_db, multi_backend_connection) -> PartitionedJobTracker:
    return PartitionedJobTracker(db=zk_db, backends=multi_backend_connection)


@pytest.fixture
def test_user() -> dict:
    return {"bearer": "oidc/egi/l3tm31n", "user_id": "john"}


@pytest.fixture
def john(test_user) -> str:
    return test_user["user_id"]


@pytest.fixture
def flask_request(test_user) -> flask.Request:
    return flask.Request(environ={"HTTP_AUTHORIZATION": f"Bearer {test_user['bearer']}"})


@pytest.fixture
def dummy1(backend1, requests_mock, test_user) -> DummyBackend:
    dummy = DummyBackend(backend_url=backend1, job_id_template="1-jb-{i}")
    dummy.setup_requests_mock(requests_mock)
    dummy.register_user(bearer_token=test_user["bearer"], user_id=test_user["user_id"])
    return dummy


@pytest.fixture
def dummy2(backend2, requests_mock, test_user) -> DummyBackend:
    dummy = DummyBackend(backend_url=backend2, job_id_template="2-jb-{i}")
    dummy.setup_requests_mock(requests_mock)
    dummy.register_user(bearer_token=test_user["bearer"], user_id=test_user["user_id"])
    return dummy


class TestPartitionedJobTracker:

    def test_create(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet({
                "status": "created",
                "message": approx_str_prefix("Created in 0:00"),
            })
        assert dummy1.jobs[john, "1-jb-0"].create == {
            "process": P12,
            "title": approx_str_contains("part 0000 (1/2)")
        }
        assert dummy1.jobs[john, "1-jb-0"].history == ["created"]
        assert dummy2.jobs[john, "2-jb-0"].create == {
            "process": P23,
            "title": approx_str_contains("part 0001 (2/2)")
        }
        assert dummy2.jobs[john, "2-jb-0"].history == ["created"]

    def test_create_error(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, requests_mock, john):
        requests_mock.post(dummy1.backend_url + "/jobs", status_code=500, json={"message": "nope"})
        requests_mock.post(dummy2.backend_url + "/jobs", status_code=500, json={"message": "meh"})

        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": approx_str_contains("{'error': 2}"),
            "timestamp": approx_now(),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "error",
            "message": approx_str_prefix("Create failed: [500] unknown: nope"),
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": approx_str_prefix("Create failed: [500] unknown: meh"),
            "timestamp": approx_now(),
        }

    def test_start(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        # Create
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })

        # Start
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "running",
            "message": approx_str_contains("{'running': 2}"),
        })
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet({
                "status": "running",
                "message": approx_str_prefix("Started in 0:00"),
            })

        assert dummy1.jobs[john, "1-jb-0"].history == ["created", "running"]
        assert dummy2.jobs[john, "2-jb-0"].history == ["created", "running"]

    def test_start_wrong_user(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        # Create
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({
            "status": "created",
            "message": approx_str_contains("{'created': 2}"),
        })

        with pytest.raises(JobNotFoundException):
            zk_tracker.start_sjobs(pjob_id=pjob_id, user_id="notjohn", flask_request=flask_request)

    def test_sync_basic(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        # Create
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (both still running)
        dummy1.set_job_status(john, "1-jb-0", "running")
        dummy2.set_job_status(john, "2-jb-0", "running")
        zk_tracker.sync(pjob_id=pjob_id, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 2}"),
            "timestamp": approx_now(),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "running",
                "message": "Upstream status: 'running'",
                "timestamp": approx_now(),
            }

        # Sync (one finished)
        dummy1.set_job_status(john, "1-jb-0", "running")
        dummy2.set_job_status(john, "2-jb-0", "finished")
        zk_tracker.sync(pjob_id=pjob_id, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 1, 'finished': 1}"),
            "timestamp": approx_now(),
            "progress": 50,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "Upstream status: 'running'",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "finished",
            "message": "Upstream status: 'finished'",
            "timestamp": approx_now(),
        }

        # Sync (both finished)
        dummy1.set_job_status(john, "1-jb-0", "finished")
        dummy2.set_job_status(john, "2-jb-0", "finished")
        zk_tracker.sync(pjob_id=pjob_id, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "finished",
            "message": approx_str_contains("{'finished': 2}"),
            "timestamp": approx_now(),
            "progress": 100,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        for sjob_id in subjobs:
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == {
                "status": "finished",
                "message": "Upstream status: 'finished'",
                "timestamp": approx_now(),
            }

        assert dummy1.jobs[john, "1-jb-0"].history == ["created", "running", "finished"]
        assert dummy2.jobs[john, "2-jb-0"].history == ["created", "running", "finished"]

    def test_sync_with_error(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        # Create
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (with error)
        dummy1.set_job_status(john, "1-jb-0", "running")
        dummy2.set_job_status(john, "2-jb-0", "error")
        zk_tracker.sync(pjob_id=pjob_id, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "running",
            "message": approx_str_contains("{'running': 1, 'error': 1}"),
            "timestamp": approx_now(),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "running",
            "message": "Upstream status: 'running'",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "Upstream status: 'error'",
            "timestamp": approx_now(),
        }

        # Sync (another error, and note invalid status too)
        dummy1.set_job_status(john, "1-jb-0", "3rr0r")
        dummy2.set_job_status(john, "2-jb-0", "error")
        zk_tracker.sync(pjob_id=pjob_id, user_id=john, flask_request=flask_request)

        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "error",
            "message": approx_str_contains("{'error': 2}"),
            "timestamp": approx_now(),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert set(subjobs.keys()) == {"0000", "0001"}
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0000") == {
            "status": "error",
            "message": "Upstream status: '3rr0r'",
            "timestamp": approx_now(),
        }
        assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id="0001") == {
            "status": "error",
            "message": "Upstream status: 'error'",
            "timestamp": approx_now(),
        }
        assert dummy1.jobs[john, "1-jb-0"].history == ["created", "running", "3rr0r"]
        assert dummy2.jobs[john, "2-jb-0"].history == ["created", "running", "error"]

    def test_sync_wrong_user(self, pjob, zk_db, zk_tracker, flask_request, dummy1, dummy2, john):
        # Create
        pjob_id = zk_tracker.create(pjob=pjob, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "created"})

        # Start
        zk_tracker.start_sjobs(pjob_id=pjob_id, user_id=john, flask_request=flask_request)
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == DictSubSet({"status": "running"})

        # Sync (both still running)
        with pytest.raises(JobNotFoundException):
            zk_tracker.sync(pjob_id=pjob_id, user_id="notjohn", flask_request=flask_request)


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
