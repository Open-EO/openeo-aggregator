import kazoo
import kazoo.exceptions
import pytest

from openeo_aggregator.testing import clock_mock, approx_now
from openeo_driver.testing import TEST_USER
from .conftest import PG12, PG23, P35


@clock_mock("2022-01-17T17:48:00Z")
class TestZooKeeperPartitionedJobDB:

    def test_insert_basic(self, pjob, zk_client, zk_db):
        pjob_id = zk_db.insert(pjob=pjob, user_id=TEST_USER)
        assert pjob_id == "pj-20220117-174800"

        data = zk_client.get_data_deserialized(drop_empty=True)
        assert data == {
            "/o-a/202201/pj-20220117-174800": {
                "created": approx_now(),
                "user_id": TEST_USER,
                "process": P35,
                "metadata": {},
                "job_options": {},
            },
            "/o-a/202201/pj-20220117-174800/status": {
                "status": "inserted",
            },
            "/o-a/202201/pj-20220117-174800/sjobs/0000": {
                "process_graph": PG12,
                "backend_id": "b1",
                "title": "Partitioned job pj-20220117-174800 part 0000 (1/2)"
            },
            "/o-a/202201/pj-20220117-174800/sjobs/0000/status": {
                "status": "inserted"
            },
            "/o-a/202201/pj-20220117-174800/sjobs/0001": {
                "process_graph": PG23,
                "backend_id": "b2",
                "title": "Partitioned job pj-20220117-174800 part 0001 (2/2)"
            },
            "/o-a/202201/pj-20220117-174800/sjobs/0001/status": {
                "status": "inserted"
            },
        }

    def test_insert_pjob_id_collision(self, pjob, zk_client, zk_db):
        with clock_mock("2022-01-17T17:48:00Z"):
            pjob_id = zk_db.insert(pjob=pjob, user_id=TEST_USER)
        assert pjob_id == "pj-20220117-174800"
        with clock_mock("2022-01-17T17:48:00Z"):
            pjob_id = zk_db.insert(pjob=pjob, user_id=TEST_USER)
        assert pjob_id == "pj-20220117-174800-1"
        with clock_mock("2022-01-17T17:48:00Z"):
            pjob_id = zk_db.insert(pjob=pjob, user_id=TEST_USER)
        assert pjob_id == "pj-20220117-174800-2"

    def test_get_pjob_metadata(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_pjob_metadata("pj-20220117-174800")

        zk_db.insert(pjob=pjob, user_id=TEST_USER)

        assert zk_db.get_pjob_metadata("pj-20220117-174800") == {
            "created": approx_now(),
            "user_id": TEST_USER,
            "process": P35,
            "metadata": {},
            "job_options": {},
        }

    def test_list_subjobs(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.list_subjobs("pj-20220117-174800")

        zk_db.insert(pjob=pjob, user_id=TEST_USER)

        assert zk_db.list_subjobs("pj-20220117-174800") == {
            "0000": {
                "process_graph": PG12,
                "backend_id": "b1",
                "title": "Partitioned job pj-20220117-174800 part 0000 (1/2)"
            },
            "0001": {
                "process_graph": PG23,
                "backend_id": "b2",
                "title": "Partitioned job pj-20220117-174800 part 0001 (2/2)"
            },
        }

    def test_set_get_backend_job_id(self, pjob, zk_db):
        pjob_id = zk_db.insert(pjob=pjob, user_id=TEST_USER)

        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_backend_job_id(pjob_id=pjob_id, sjob_id="0000")

        zk_db.set_backend_job_id(pjob_id=pjob_id, sjob_id="0000", job_id="b1-job-123")

        assert zk_db.get_backend_job_id(pjob_id=pjob_id, sjob_id="0000") == "b1-job-123"

    def test_set_get_pjob_status(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_pjob_status(pjob_id="pj-20220117-174800")

        zk_db.insert(pjob=pjob, user_id=TEST_USER)

        status = zk_db.get_pjob_status(pjob_id="pj-20220117-174800")
        assert status == {"status": "inserted"}

        zk_db.set_pjob_status(pjob_id="pj-20220117-174800", status="running", message="goin' on")
        status = zk_db.get_pjob_status(pjob_id="pj-20220117-174800")
        assert status == {"status": "running", "message": "goin' on", "timestamp": approx_now(), "progress": None}

        zk_db.set_pjob_status(pjob_id="pj-20220117-174800", status="running", message="goin' on", progress=45)
        status = zk_db.get_pjob_status(pjob_id="pj-20220117-174800")
        assert status == {"status": "running", "message": "goin' on", "timestamp": approx_now(), "progress": 45}

    def test_set_get_sjob_status(self, pjob, zk_db):
        with pytest.raises(kazoo.exceptions.NoNodeError):
            zk_db.get_sjob_status(pjob_id="pj-20220117-174800", sjob_id="0000")

        zk_db.insert(pjob=pjob, user_id=TEST_USER)

        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800", sjob_id="0000")
        assert status == {"status": "inserted"}

        zk_db.set_sjob_status(pjob_id="pj-20220117-174800", sjob_id="0000", status="running", message="goin' on")
        status = zk_db.get_sjob_status(pjob_id="pj-20220117-174800", sjob_id="0000")
        assert status == {"status": "running", "message": "goin' on", "timestamp": approx_now()}
