import pytest

from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB
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
