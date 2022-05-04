import datetime
import pytest

from openeo.util import rfc3339
from openeo_aggregator.backend import AggregatorBackendImplementation, AggregatorBatchJobs
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB
from openeo_aggregator.testing import clock_mock, approx_str_contains, approx_str_prefix
from openeo_aggregator.utils import BoundingBox
from openeo_driver.testing import DictSubSet
from .conftest import PG35, P35, TEST_USER, TEST_USER_BEARER_TOKEN, OTHER_TEST_USER_BEARER_TOKEN, DummyBackend
from .test_splitting import check_tiling_coordinate_histograms


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
    dummy = DummyBackend(requests_mock=requests_mock, backend_url=backend1, job_id_template="1-jb-{i}")
    dummy.setup_basic_requests_mocks()
    dummy.register_user(bearer_token=TEST_USER_BEARER_TOKEN, user_id=TEST_USER)
    return dummy


class TestFlimsyBatchJobSplitting:
    now = _Now("2022-01-19T12:34:56Z")

    @now.mock
    def test_create_job_basic(self, api100, zk_db, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
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

        assert zk_db.get_pjob_metadata(user_id=TEST_USER, pjob_id=pjob_id) == {
            "user_id": TEST_USER,
            "created": self.now.epoch,
            "process": P35,
            "metadata": {"title": "3+5", "description": "Addition of 3 and 5", "plan": "free"},
            "job_options": {"split_strategy": "flimsy"},
        }
        assert zk_db.get_pjob_status(user_id=TEST_USER, pjob_id=pjob_id) == {
            "status": "created",
            "message": approx_str_contains("{'created': 1}"),
            "timestamp": pytest.approx(self.now.epoch, abs=5),
            "progress": 0,
        }

        assert zk_db.list_subjobs(user_id=TEST_USER, pjob_id=pjob_id) == {"0000": {
            "backend_id": "b1",
            "process_graph": PG35,
            "title": "Partitioned job pj-20220119-123456 part 0000 (1/1)"
        }}
        assert zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == "1-jb-0"
        assert zk_db.get_sjob_status(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == {
            "status": "created",
            "message": approx_str_prefix("Created in 0:00"),
            "timestamp": pytest.approx(self.now.epoch, abs=5)
        }

    @now.mock
    def test_create_job_preprocessing(self, api100, zk_db, dummy1):
        """Issue #19: strip backend prefix from job_id in load_result"""
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b1-b6tch-j08"}, "result": True}}
        res = api100.post("/jobs", json={
            "process": {"process_graph": pg},
            "job_options": {"split_strategy": "flimsy"}
        })
        res.assert_status_code(201)

        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        job_id = zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id="pj-20220119-123456", sjob_id="0000")
        assert dummy1.get_job_data(TEST_USER, job_id).create["process"]["process_graph"] == {
            "load": {"process_id": "load_result", "arguments": {"id": "b6tch-j08"}, "result": True}
        }

    @now.mock
    def test_create_and_list_job(self, api100, zk_db, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs").assert_status_code(200)
        assert res.json == {
            "jobs": [
                {"id": "b1-1-jb-0", "created": self.now.rfc3339, "status": "created"},
                {"id": expected_job_id, "created": self.now.rfc3339, "status": "created", "progress": 0}
            ],
            "federation:missing": ["b2"],
            "links": [],
        }

    def test_describe_wrong_user(self, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "created"})

        # Wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.get(f"/jobs/{job_id}").assert_error(404, "JobNotFound")

    @now.mock
    def test_create_job_failed_backend(self, api100, zk_db, requests_mock, dummy1):
        requests_mock.post(dummy1.backend_url + "/jobs", status_code=500, json={"code": "Internal", "message": "nope"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
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

        assert zk_db.get_pjob_status(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "status": "error",
        })
        assert zk_db.get_sjob_status(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == DictSubSet({
            "status": "error",
            "message": "Create failed: [500] Internal: nope",
        })

        res = api100.get(f"/jobs/{expected_job_id}/logs").assert_status_code(200)
        assert res.json == {
            "logs": [{"id": "0000-0", "level": "error", "message": approx_str_contains("NoJobIdForSubJob")}],
            "links": [],
        }

    @now.mock
    def test_start_job(self, api100, zk_db, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "created", "progress": 0})

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)

        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        assert zk_db.get_pjob_metadata(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "created": self.now.epoch,
            "process": P35,
        })
        assert zk_db.get_pjob_status(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "status": "running",
            "message": approx_str_contains("{'running': 1}"),
        })
        assert zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == "1-jb-0"
        assert zk_db.get_sjob_status(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == DictSubSet(
            {"status": "running"})

    def test_start_job_wrong_user(self, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        res = api100.get(f"/jobs/{job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": job_id, "status": "created"})

        # Start job as wrong user
        api100.set_auth_bearer_token(OTHER_TEST_USER_BEARER_TOKEN)
        api100.post(f"/jobs/{job_id}/results").assert_error(404, "JobNotFound")

    @now.mock
    def test_sync_job(self, api100, zk_db, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
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

        assert zk_db.get_pjob_metadata(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "created": self.now.epoch,
            "process": P35,
        })
        assert zk_db.get_pjob_status(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "status": "finished",
            "message": approx_str_contains("{'finished': 1}"),
        })
        assert zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == "1-jb-0"
        assert zk_db.get_sjob_status(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000") == DictSubSet(
            {"status": "finished"})

    def test_sync_job_wrong_user(self, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
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
    def test_job_results(self, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
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
        dummy1.setup_assets(job_id="1-jb-0", assets=["preview.png", "res001.tif", "res002.tif"])

        res = api100.get(f"/jobs/{expected_job_id}/results").assert_status_code(200)
        assert res.json == DictSubSet({
            "id": expected_job_id,
            "assets": {
                "0000-preview.png": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-0/results/preview.png"}),
                "0000-res001.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-0/results/res001.tif"}),
                "0000-res002.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-0/results/res002.tif"}),
            }
        })

    def test_job_results_wrong_user(self, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Submit job
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
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
    def test_get_logs(self, api100, requests_mock, dummy1):
        requests_mock.get(dummy1.backend_url + "/jobs/1-jb-0/logs", json={
            "logs": [{"id": "123", "level": "info", "message": "Created job. You're welcome."}]
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "3+5",
            "description": "Addition of 3 and 5",
            "process": P35,
            "plan": "free",
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)
        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        res = api100.get(f"/jobs/{expected_job_id}/logs").assert_status_code(200)
        assert res.json == {
            "logs": [{"id": "0000-123", "level": "info", "message": "Created job. You're welcome."}],
            "links": [],
        }

    @now.mock
    def test_get_logs_wrong_user(self, api100, requests_mock, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={
            "process": P35,
            "job_options": {"split_strategy": "flimsy"}
        }).assert_status_code(201)
        expected_job_id = "agg-pj-20220119-123456"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        api100.set_auth_bearer_token(token=OTHER_TEST_USER_BEARER_TOKEN)
        api100.get(f"/jobs/{expected_job_id}/logs").assert_error(404, "JobNotFound")


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
    def test_create_job_basic(self, flask_app, api100, zk_db, dummy1):
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

        assert zk_db.get_pjob_metadata(user_id=TEST_USER, pjob_id=pjob_id) == DictSubSet({
            "user_id": TEST_USER,
            "created": self.now.epoch,
            "process": {"process_graph": self.PG_MOL},
            "metadata": {
                "title": "Mol", "plan": "free",
                "_tiling_geometry": DictSubSet({
                    "global_spatial_extent": DictSubSet({"west": 4.9})
                }),
            },
            "job_options": {"tile_grid": "utm-10km"},
        })
        assert zk_db.get_pjob_status(user_id=TEST_USER, pjob_id=pjob_id) == {
            "status": "created",
            "message": approx_str_contains("{'created': 9}"),
            "timestamp": pytest.approx(self.now.epoch, abs=5),
            "progress": 0,
        }
        subjobs = zk_db.list_subjobs(user_id=TEST_USER, pjob_id=pjob_id)
        assert len(subjobs) == 9
        dummy_jobs = []
        tiles = []
        for sjob_id, subjob_metadata in subjobs.items():
            assert zk_db.get_sjob_status(user_id=TEST_USER, pjob_id=pjob_id, sjob_id=sjob_id) == DictSubSet(
                {"status": "created"})
            job_id = zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id=pjob_id, sjob_id=sjob_id)
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

    @now.mock
    def test_create_job_preprocessing(self, flask_app, api100, zk_db, dummy1):
        """Issue #19: strip backend prefix from job_id in load_result"""
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Process graph with load_result
        pg = {
            "lr": {"process_id": "load_result", "arguments": {"id": "b1-b6tch-j08"}},
            "fb": {"process_id": "filter_bbox", "arguments": {
                "data": {"from_node": "lr"},
                "extent": {"west": 4.9, "south": 51.1, "east": 4.91, "north": 51.11},
            }},
            "sr": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "fb"}, "format": "GTiff"},
                "result": True,
            }
        }
        res = api100.post("/jobs", json={
            "process": {"process_graph": pg},
            "job_options": {"tile_grid": "utm-10km"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
        assert res.headers["OpenEO-Identifier"] == expected_job_id

        job_id = zk_db.get_backend_job_id(user_id=TEST_USER, pjob_id=pjob_id, sjob_id="0000")
        pg = dummy1.get_job_data(TEST_USER, job_id).create["process"]["process_graph"]
        assert pg["lr"]["arguments"]["id"] == "b6tch-j08"

    @now.mock
    def test_job_results_basic(self, flask_app, api100, dummy1):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        res = api100.post("/jobs", json={
            "title": "Mol",
            "process": {"process_graph": self.PG_MOL},
            "plan": "free",
            "job_options": {"tile_grid": "utm-10km"}
        }).assert_status_code(201)

        pjob_id = "pj-20220119-123456"
        expected_job_id = f"agg-{pjob_id}"
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

        # Start job
        api100.post(f"/jobs/{expected_job_id}/results").assert_status_code(202)
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 0})

        # Status check: Partially finished
        for i in range(5):
            dummy1.set_job_status(TEST_USER, f"1-jb-{i}", "finished")
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "running", "progress": 55})

        # Status check: Fully finished
        for i in range(9):
            dummy1.set_job_status(TEST_USER, f"1-jb-{i}", "finished")
            dummy1.setup_assets(job_id=f"1-jb-{i}", assets=["result.tif"])
        res = api100.get(f"/jobs/{expected_job_id}").assert_status_code(200)
        assert res.json == DictSubSet({"id": expected_job_id, "status": "finished", "progress": 100})

        # Get results
        res = api100.get(f"/jobs/{expected_job_id}/results").assert_status_code(200)
        assert res.json == DictSubSet({
            "id": expected_job_id,
            "assets": {
                "0000-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-0/results/result.tif"}),
                "0001-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-1/results/result.tif"}),
                "0002-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-2/results/result.tif"}),
                "0003-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-3/results/result.tif"}),
                "0004-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-4/results/result.tif"}),
                "0005-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-5/results/result.tif"}),
                "0006-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-6/results/result.tif"}),
                "0007-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-7/results/result.tif"}),
                "0008-result.tif": DictSubSet({"href": dummy1.backend_url + "/jobs/1-jb-8/results/result.tif"}),
                "tile_grid.geojson": DictSubSet({
                    "href": "http://oeoa.test/openeo/1.0.0/jobs/agg-pj-20220119-123456/results/assets/tile_grid.geojson",
                    "type": "application/geo+json",
                })
            },
            "geometry": DictSubSet({
                "type": "GeometryCollection",
                "geometries": [DictSubSet({"type": "Polygon"}), DictSubSet({"type": "MultiPolygon"})]
            })
        })

        res = api100.get("/jobs/agg-pj-20220119-123456/results/assets/tile_grid.geojson").assert_status_code(200)
        assert res.json == DictSubSet({"type": "FeatureCollection"})

    # TODO: more/full TileGridSplitter batch job tests
