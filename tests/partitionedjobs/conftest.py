import collections
import pytest
import re
import requests
from typing import Dict, Tuple

from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB
from openeo_driver.errors import JobNotFoundException, TokenInvalidException
from openeo_driver.users.auth import HttpAuthHandler

PG12 = {
    "add": {"process_id": "add", "arguments": {"X": 1, "y": 2}, "result": True}
}
PG23 = {
    "add": {"process_id": "add", "arguments": {"X": 2, "y": 3}, "result": True}
}
PG35 = {
    "add": {"process_id": "add", "arguments": {"X": 3, "y": 5}, "result": True}
}
P12 = {"process_graph": PG12}
P23 = {"process_graph": PG23}
P35 = {"process_graph": PG35}

OTHER_TEST_USER = "Someb0dyEl53"
OTHER_TEST_USER_BEARER_TOKEN = "basic//" + HttpAuthHandler.build_basic_access_token(user_id=OTHER_TEST_USER)


@pytest.fixture
def zk_db(zk_client) -> ZooKeeperPartitionedJobDB:
    return ZooKeeperPartitionedJobDB(client=zk_client, prefix="/o-a/")


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


DummyBatchJobData = collections.namedtuple("DummyJobData", ["create", "history"])


class DummyBackend:
    """Dummy remote backend that with basic batch job management skills"""

    # TODO: move this to openeo_aggregator.testing?

    def __init__(self, backend_url: str, job_id_template: str = "job{i}"):
        self.backend_url = backend_url
        self.job_id_template = job_id_template
        self.jobs: Dict[Tuple[str, str], DummyBatchJobData] = {}
        self.users: Dict[str, str] = {}

    def register_user(self, bearer_token: str, user_id: str):
        self.users[bearer_token] = user_id

    def get_user_id(self, request: requests.Request):
        bearer_token = request.headers["authorization"].split(" ")[-1]
        if bearer_token not in self.users:
            raise TokenInvalidException
        return self.users[bearer_token]

    def get_job_data(self, user_id, job_id) -> DummyBatchJobData:
        if (user_id, job_id) not in self.jobs:
            raise JobNotFoundException
        return self.jobs[user_id, job_id]

    def setup_requests_mock(self, requests_mock):
        requests_mock.post(self.backend_url + "/jobs", text=self._handle_post_jobs)
        requests_mock.post(
            re.compile(re.escape(self.backend_url) + "/jobs/(?P<job_id>[a-z0-9-]+)/results$"),
            text=self._handle_post_jobs_jobid_result,
        )
        requests_mock.get(
            re.compile(re.escape(self.backend_url) + "/jobs/(?P<job_id>[a-z0-9-]+)$"),
            json=self._handle_get_jobs_jobid,
        )

    def set_job_status(self, user_id: str, job_id: str, status: str):
        job_data = self.get_job_data(user_id, job_id)
        if status != job_data.history[-1]:
            job_data.history.append(status)

    def _handle_post_jobs(self, request: requests.Request, context):
        """`POST /jobs` handler (create job)"""
        user_id = self.get_user_id(request)
        job_id = self.job_id_template.format(i=len(self.jobs))
        assert (user_id, job_id) not in self.jobs
        self.jobs[user_id, job_id] = DummyBatchJobData(create=request.json(), history=["created"])
        context.headers["Location"] = f"{self.backend_url}/jobs/{job_id}"
        context.headers["OpenEO-Identifier"] = job_id
        context.status_code = 201

    def _handle_post_jobs_jobid_result(self, request: requests.Request, context):
        """`POST /jobs/<job_id>/result` handler (start job)"""
        user_id = self.get_user_id(request)
        job_id = re.search("/jobs/(?P<job_id>[a-z0-9-]+)/results", request.path).group("job_id")
        self.set_job_status(user_id, job_id, "running")
        context.status_code = 202

    def _handle_get_jobs_jobid(self, request: requests.Request, context):
        """`GET /jobs/<job_id>/result` handler (get job status)"""
        user_id = self.get_user_id(request)
        job_id = re.search("/jobs/(?P<job_id>[a-z0-9-]+)", request.path).group("job_id")
        job_data = self.get_job_data(user_id, job_id)
        return {"id": job_id, "status": job_data.history[-1]}
