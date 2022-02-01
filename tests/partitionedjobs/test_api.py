import datetime
import pytest

from openeo.util import rfc3339
from openeo_aggregator.backend import AggregatorBackendImplementation, AggregatorBatchJobs
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB
from openeo_aggregator.testing import clock_mock, approx_str_contains
from openeo_aggregator.utils import BoundingBox
from openeo_driver.testing import TEST_USER_BEARER_TOKEN, DictSubSet, TEST_USER
from .conftest import PG35, P35, OTHER_TEST_USER_BEARER_TOKEN
from .test_splitting import check_tiling_coordinate_histograms
from .test_tracking import DummyBackend


@pytest.fixture()
def zk_db(backend_implementation: AggregatorBackendImplementation) -> ZooKeeperPartitionedJobDB:
    batch_jobs: AggregatorBatchJobs = backend_implementation.batch_jobs
    return batch_jobs.partitioned_job_tracker._db


class _Now:
    """Helper to mock "now" to given datetime"""

    # TODO: move to testing utilities and reuse  more?

    def __init__(self, date: str):
        self.rfc3339 = rfc3339.normalize(date)
        self.datetime = rfc3339.parse_datetime(self.rfc3339).replace(tzinfo=datetime.timezone.utc)
        self.epoch = self.datetime.timestamp()
        self.mock = clock_mock(self.rfc3339)


@pytest.fixture
def dummy1(backend1, requests_mock) -> DummyBackend:
    dummy = DummyBackend(backend_url=backend1, job_id_template="1-jb-{i}")
    dummy.setup_requests_mock(requests_mock)
    dummy.register_user(bearer_token=TEST_USER_BEARER_TOKEN, user_id=TEST_USER)
    return dummy


class TestFlimsyBatchJobSplitting:
    now = _Now("2022-01-19T12:34:56Z")

    @now.mock
    def test_create_job_basic(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["Location"] == f"http://oeoa.test/openeo/1.0.0/jobs/{expected_job_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == {
            "id": expected_job_id,
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "status": "created",
            "created": self.now.rfc3339,
            "progress": 0,
        }

        # TODO: these unit tests should not really care about the zookeeper state
        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        zk_prefix = "/o-a/pj/v1/202201/pj-20220119-123456"
        assert zk_data[zk_prefix] == {
            "user_id": TEST_USER,
            "created": self.now.epoch,
            "process": P35,
            "metadata": {"title": "3+5", "description": "Addition of 3 and 5", "plan": "free"},
            "job_options": {"_jobsplitting": True},
        }
        assert zk_data[zk_prefix + "/status"] == {
            "status": "created",
            "message": approx_str_contains("{'created': 1}"),
            "timestamp": pytest.approx(self.now.epoch, abs=5),
            "progress": 0,
        }
        assert zk_data[zk_prefix + "/sjobs/0000"] == {
            "backend_id": "b1",
            "process_graph": PG35,
            "title": "Partitioned job pj-20220119-123456 part 0000 (1/1)"
        }
        assert zk_data[zk_prefix + "/sjobs/0000/job_id"] == {
            "job_id": "1-jb-0",
        }
        assert zk_data[zk_prefix + "/sjobs/0000/status"] == {
            "status": "created",
            "message": "created",
            "timestamp": pytest.approx(self.now.epoch, abs=5)
        }

    def test_describe_wrong_user(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "created"})

        # Wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.get(f"/jobs/{job_id}").assert_error(404, "JobNotFound")

    @now.mock
    def test_create_job_failed_backend(self, api100, backend1, zk_client, requests_mock, dummy1):
        requests_mock.post(backend1 + "/jobs", status_code=500, json={"code": "Internal", "message": "nope"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == {
            "id": expected_job_id,
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "status": "error",
            "created": self.now.rfc3339,
            "progress": 0,
        }

        # TODO: these unit tests should not really care about the zookeeper state
        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        zk_prefix = "/o-a/pj/v1/202201/pj-20220119-123456"
        assert zk_data[zk_prefix + "/status"] == DictSubSet({
            "status": "error",
        })
        assert zk_data[zk_prefix + "/sjobs/0000/status"] == DictSubSet({
            "status": "error",
            "message": "Create failed: [500] Internal: nope",
        })

        res = api100.get(f"/jobs/{expected_job_id}/logs").assert_status_code(200)
        assert res.json == {
            "logs": [{"id": "0000-0", "level": "error", "message": approx_str_contains("NoJobIdForSubJob")}],
            "links": [],
        }

    @now.mock
    def test_start_job(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "created", "progress": 0})

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        # TODO: these unit tests should not really care about the zookeeper state
        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        zk_prefix = "/o-a/pj/v1/202201/pj-20220119-123456"
        assert zk_data[zk_prefix] == DictSubSet({
            "created": self.now.epoch,
            "process": P35,
        })
        assert zk_data[zk_prefix + "/status"] == DictSubSet({
            "status": "running",
            "message": approx_str_contains("{'running': 1}"),
        })
        assert zk_data[zk_prefix + "/sjobs/0000/job_id"] == DictSubSet({"job_id": "1-jb-0"})
        assert zk_data[zk_prefix + "/sjobs/0000/status"] == DictSubSet({"status": "running"})

    def test_start_job_wrong_user(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "created"})

        # Start job as wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.post(f"/jobs/{job_id}/results").assert_error(404, "JobNotFound")

    @now.mock
    def test_sync_job(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "created"})

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        # Status check: still running
        dummy1.set_job_status(TEST_USER, '1-jb-0', "running")
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        # Status check: finished
        dummy1.set_job_status(TEST_USER, '1-jb-0', "finished")
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "finished", "progress": 100})

        # TODO: these unit tests should not really care about the zookeeper state
        zk_data = zk_client.get_data_deserialized(drop_empty=True)
        zk_prefix = "/o-a/pj/v1/202201/pj-20220119-123456"
        assert zk_data[zk_prefix] == DictSubSet({
            "created": self.now.epoch,
            "process": P35,
        })
        assert zk_data[zk_prefix + "/status"] == DictSubSet({
            "status": "finished",
            "message": approx_str_contains("{'finished': 1}"),
        })
        assert zk_data[zk_prefix + "/sjobs/0000/job_id"] == DictSubSet({"job_id": "1-jb-0"})
        assert zk_data[zk_prefix + "/sjobs/0000/status"] == DictSubSet({"status": "finished"})

    def test_sync_job_wrong_user(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        # Start job
        api100.post(f"/jobs/{job_id}/results").assert_status_code(202)
        api100.get(f"/jobs/{job_id}").assert_status_code(200)

        # Status check: still running
        dummy1.set_job_status(TEST_USER, "1-jb-0", "running")
        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "running"})

        # Status check as wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.get(f"/jobs/{job_id}").assert_error(404, "JobNotFound")

    @now.mock
    def test_job_results(self, api100, backend1, zk_client, requests_mock, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        # Status check: finished
        dummy1.set_job_status(TEST_USER, "1-jb-0", "finished")
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "finished", "progress": 100})

        # Get results
        # TODO: move this mock to DummyBackend
        requests_mock.get(backend1 + "/jobs/1-jb-0/results", json={
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
                "0000-preview.png": DictSubSet({"href": backend1 + "/jobs/1j0b/results/preview.png"}),
                "0000-res001.tif": DictSubSet({"href": backend1 + "/jobs/1j0b/results/res001.tiff"}),
                "0000-res002.tif": DictSubSet({"href": backend1 + "/jobs/1j0b/results/res002.tiff"}),
            }
        })

    def test_job_results_wrong_user(self, api100, backend1, zk_client, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        # Start job
        api100.post(f"/jobs/{job_id}/results").assert_status_code(202)

        # Status check: finished
        dummy1.set_job_status(TEST_USER, "1-jb-0", "finished")
        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "finished"})

        # Get results as wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.get(f"/jobs/{job_id}/results").assert_error(404, "JobNotFound")

    @now.mock
    def test_get_logs(self, api100, backend1, zk_client, requests_mock, dummy1):
        requests_mock.get(backend1 + "/jobs/1-jb-0/logs", json={
            "logs": [{"id": "123", "level": "info", "message": "Created job. You're welcome."}]
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"_jobsplitting": True}
        }).assert_status_code(201)
        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}/logs").assert_status_code(200)
        assert res.json == {
            "logs": [{"id": "0000-123", "level": "info", "message": "Created job. You're welcome."}],
            "links": [],
        }


class TestTileGridBatchJobSplitting:
    now = _Now("2022-01-19T12:34:56Z")

    PG_MOL = {
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "S2",
                # covers 9 (3x3) utm-10km tiles
                "spatial_extent": {"west": 4.9, "south": 51.1, "east": 5.2, "north": 51.3},
            }
        },
        "sr": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "lc"}, "format": "GTiff"},
            "result": True,
        }
    }

    @now.mock
    def test_create_job_basic(self, flask_app, api100, backend1, zk_client, zk_db, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "Mol",
            "process": {"process_graph": self.PG_MOL},
            "plan": "free",
            "job_options": {"tile_grid": "utm-10km"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
        assert res.headers["Location"] == f"http://oeoa.test/openeo/1.0.0/jobs/{expected_job_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == {
            "id": expected_job_id,
            "title": "Mol",
            "process": {"process_graph": self.PG_MOL},
            "status": "created",
            "created": self.now.rfc3339,
            "progress": 0,
        }

        assert zk_db.get_pjob_metadata(pjob_id=pjob_id) == DictSubSet({
            "user_id": TEST_USER,
            "created": self.now.epoch,
            "process": {"process_graph": self.PG_MOL},
            "metadata": {"title": "Mol", "plan": "free"},
            "job_options": {"tile_grid": "utm-10km"},
        })
        assert zk_db.get_pjob_status(pjob_id=pjob_id) == {
            "status": "created",
            "message": approx_str_contains("{'created': 9}"),
            "timestamp": pytest.approx(self.now.epoch, abs=5),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(pjob_id=pjob_id)
        assert len(subjobs) == 9
        dummy_jobs = []
        tiles = []
        for sjob_id, subjob_metadata in subjobs.items():
            assert zk_db.get_sjob_status(pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet({"status": "created"})
            job_id = zk_db.get_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id)
            dummy_jobs.append(job_id)
            assert dummy1.get_job_status(TEST_USER, job_id) == "created"
            pg = dummy1.get_job_data(TEST_USER, job_id).create["process"]["process_graph"]
            new_node = next(v for k, v in pg.items() if k.startswith("_agg"))
            extent = new_node["arguments"]["extent"]
            assert extent["crs"] == "epsg:32631"
            tiles.append(BoundingBox.from_dict(extent))
        assert sorted(dummy_jobs) == [f"1-jb-{i}" for i in range(9)]
        # Rudimentary coordinate checks
        check_tiling_coordinate_histograms(tiles)

    # TODO: more/full TileGridSplitter batch job tests