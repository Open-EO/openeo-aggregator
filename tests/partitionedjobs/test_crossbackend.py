import dataclasses
import re
import types
from typing import Dict, List, Optional
from unittest import mock

import openeo
import pytest
import requests
import requests_mock
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet

from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.crossbackend import (
    CrossBackendSplitter,
    SubGraphId,
    run_partitioned_job,
)


class TestCrossBackendSplitter:
    def test_split_simple(self):
        process_graph = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: "foo")
        res = splitter.split({"process_graph": process_graph})

        assert res.subjobs == {"main": SubJob(process_graph, backend_id=None)}
        assert res.dependencies == {}

    def test_split_streaming_simple(self):
        process_graph = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: "foo")
        res = splitter.split_streaming(process_graph)
        assert isinstance(res, types.GeneratorType)
        assert list(res) == [("main", SubJob(process_graph, backend_id=None), [])]

    def test_split_basic(self):
        process_graph = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
            "mc1": {
                "process_id": "merge_cubes",
                "arguments": {
                    "cube1": {"from_node": "lc1"},
                    "cube2": {"from_node": "lc2"},
                },
            },
            "sr1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        res = splitter.split({"process_graph": process_graph})

        assert res.subjobs == {
            "main": SubJob(
                process_graph={
                    "lc1": {
                        "process_id": "load_collection",
                        "arguments": {"id": "B1_NDVI"},
                    },
                    "lc2": {
                        "process_id": "load_result",
                        "arguments": {"id": "_placeholder:B2:lc2"},
                    },
                    "mc1": {
                        "process_id": "merge_cubes",
                        "arguments": {
                            "cube1": {"from_node": "lc1"},
                            "cube2": {"from_node": "lc2"},
                        },
                    },
                    "sr1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                        "result": True,
                    },
                },
                backend_id="B1",
            ),
            "B2:lc2": SubJob(
                process_graph={
                    "lc2": {
                        "process_id": "load_collection",
                        "arguments": {"id": "B2_FAPAR"},
                    },
                    "sr1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                        "result": True,
                    },
                },
                backend_id="B2",
            ),
        }
        assert res.dependencies == {"main": ["B2:lc2"]}

    def test_split_streaming_basic(self):
        process_graph = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
            "mc1": {
                "process_id": "merge_cubes",
                "arguments": {
                    "cube1": {"from_node": "lc1"},
                    "cube2": {"from_node": "lc2"},
                },
            },
            "sr1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        result = splitter.split_streaming(process_graph)
        assert isinstance(result, types.GeneratorType)

        assert list(result) == [
            (
                "B2:lc2",
                SubJob(
                    process_graph={
                        "lc2": {
                            "process_id": "load_collection",
                            "arguments": {"id": "B2_FAPAR"},
                        },
                        "sr1": {
                            "process_id": "save_result",
                            "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                            "result": True,
                        },
                    },
                    backend_id="B2",
                ),
                [],
            ),
            (
                "main",
                SubJob(
                    process_graph={
                        "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
                        "lc2": {"process_id": "load_result", "arguments": {"id": "_placeholder:B2:lc2"}},
                        "mc1": {
                            "process_id": "merge_cubes",
                            "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
                        },
                        "sr1": {
                            "process_id": "save_result",
                            "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                            "result": True,
                        },
                    },
                    backend_id="B1",
                ),
                ["B2:lc2"],
            ),
        ]

    def test_split_streaming_get_replacement(self):
        process_graph = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
            "lc3": {"process_id": "load_collection", "arguments": {"id": "B3_SCL"}},
            "merge": {
                "process_id": "merge",
                "arguments": {
                    "cube1": {"from_node": "lc1"},
                    "cube2": {"from_node": "lc2"},
                    "cube3": {"from_node": "lc3"},
                },
                "result": True,
            },
        }
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: cid.split("_")[0])

        batch_jobs = {}

        def get_replacement(node_id: str, node: dict, subgraph_id: SubGraphId) -> dict:
            return {
                node_id: {
                    "process_id": "load_batch_job",
                    "arguments": {"batch_job": batch_jobs[subgraph_id]},
                }
            }

        substream = splitter.split_streaming(process_graph, get_replacement=get_replacement)

        result = []
        for subgraph_id, subjob, dependencies in substream:
            batch_jobs[subgraph_id] = f"job-{111 * (len(batch_jobs) + 1)}"
            result.append((subgraph_id, subjob, dependencies))

        assert list(result) == [
            (
                "B2:lc2",
                SubJob(
                    process_graph={
                        "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
                        "sr1": {
                            "process_id": "save_result",
                            "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                            "result": True,
                        },
                    },
                    backend_id="B2",
                ),
                [],
            ),
            (
                "B3:lc3",
                SubJob(
                    process_graph={
                        "lc3": {"process_id": "load_collection", "arguments": {"id": "B3_SCL"}},
                        "sr1": {
                            "process_id": "save_result",
                            "arguments": {"data": {"from_node": "lc3"}, "format": "GTiff"},
                            "result": True,
                        },
                    },
                    backend_id="B3",
                ),
                [],
            ),
            (
                "main",
                SubJob(
                    process_graph={
                        "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
                        "lc2": {"process_id": "load_batch_job", "arguments": {"batch_job": "job-111"}},
                        "lc3": {"process_id": "load_batch_job", "arguments": {"batch_job": "job-222"}},
                        "merge": {
                            "process_id": "merge",
                            "arguments": {
                                "cube1": {"from_node": "lc1"},
                                "cube2": {"from_node": "lc2"},
                                "cube3": {"from_node": "lc3"},
                            },
                            "result": True,
                        },
                    },
                    backend_id="B1",
                ),
                ["B2:lc2", "B3:lc3"],
            ),
        ]


@dataclasses.dataclass
class _FakeJob:
    pg: Optional[dict] = None
    created: bool = False
    started: bool = False
    status_train: List[str] = dataclasses.field(
        default_factory=lambda: [
            JOB_STATUS.QUEUED,
            JOB_STATUS.RUNNING,
            JOB_STATUS.FINISHED,
        ]
    )


class _FakeAggregator:
    def __init__(self, url: str = "http://oeoa.test"):
        self.url = url
        self.jobs: Dict[str, _FakeJob] = {}
        self.job_status_stacks = {}

    def setup_requests_mock(self, requests_mock: requests_mock.Mocker):
        requests_mock.get(f"{self.url}/", json={"api_version": "1.1.0"})
        requests_mock.post(f"{self.url}/jobs", text=self._handle_job_create)
        requests_mock.get(re.compile("/jobs/([^/]*)$"), json=self._handle_job_status)
        requests_mock.post(re.compile("/jobs/([^/]*)/results$"), text=self._handle_job_start)

    def _handle_job_create(self, request: requests.Request, context):
        pg = request.json()["process"]["process_graph"]
        # Determine job id based on used collection id
        cids = "-".join(sorted(n["arguments"]["id"] for n in pg.values() if n["process_id"] == "load_collection"))
        assert cids
        job_id = f"job-{cids}".lower()
        if job_id in self.jobs:
            assert not self.jobs[job_id].created
            self.jobs[job_id].pg = pg
            self.jobs[job_id].created = True
        else:
            self.jobs[job_id] = _FakeJob(pg=pg, created=True)

        context.headers["Location"] = f"{self.url}/jobs/{job_id}"
        context.headers["OpenEO-Identifier"] = job_id
        context.status_code = 201

    def _job_id_from_request(self, request: requests.Request) -> str:
        return re.search(r"/jobs/([^/]*)", request.path).group(1).lower()

    def _handle_job_status(self, request: requests.Request, context):
        job_id = self._job_id_from_request(request)
        job = self.jobs[job_id]
        assert job.created
        if job.started:
            if len(job.status_train) > 1:
                status = job.status_train.pop(0)
            elif len(job.status_train) == 1:
                status = job.status_train[0]
            else:
                status = JOB_STATUS.ERROR
        else:
            status = JOB_STATUS.CREATED
        return {"status": status}

    def _handle_job_start(self, request: requests.Request, context):
        job_id = self._job_id_from_request(request)
        self.jobs[job_id].started = True
        context.status_code = 202


class TestRunPartitionedJobs:
    @pytest.fixture
    def aggregator(self, requests_mock) -> _FakeAggregator:
        aggregator = _FakeAggregator()
        aggregator.setup_requests_mock(requests_mock=requests_mock)
        return aggregator

    def test_simple(self, aggregator: _FakeAggregator):
        connection = openeo.Connection(aggregator.url)
        pjob = PartitionedJob(
            process={},
            metadata={},
            job_options={},
            subjobs={
                "one": SubJob(
                    process_graph={
                        "lc": {
                            "process_id": "load_collection",
                            "arguments": {"id": "S2"},
                        }
                    },
                    backend_id="b1",
                )
            },
        )

        with mock.patch("time.sleep") as sleep:
            res = run_partitioned_job(pjob=pjob, connection=connection, fail_fast=True)
        assert res == {"one": DictSubSet({"state": "finished"})}
        assert sleep.call_count >= 1
        assert set(aggregator.jobs.keys()) == {"job-s2"}
        assert aggregator.jobs["job-s2"].pg == {
            "lc": {
                "process_id": "load_collection",
                "arguments": {"id": "S2"},
            }
        }

    def test_basic(self, aggregator: _FakeAggregator):
        process_graph = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "B2_FAPAR"}},
            "mc1": {
                "process_id": "merge_cubes",
                "arguments": {
                    "cube1": {"from_node": "lc1"},
                    "cube2": {"from_node": "lc2"},
                },
            },
            "sr1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        pjob: PartitionedJob = splitter.split({"process_graph": process_graph})

        connection = openeo.Connection(aggregator.url)
        with mock.patch("time.sleep") as sleep:
            res = run_partitioned_job(pjob=pjob, connection=connection, fail_fast=True)

        assert res == {
            "main": DictSubSet({"state": "finished"}),
            "B2:lc2": DictSubSet({"state": "finished"}),
        }
        assert sleep.call_count >= 1
        assert set(aggregator.jobs.keys()) == {"job-b2_fapar", "job-b1_ndvi"}
        assert aggregator.jobs["job-b1_ndvi"].pg == {
            "lc1": {
                "process_id": "load_collection",
                "arguments": {"id": "B1_NDVI"},
            },
            "lc2": {
                "process_id": "load_result",
                "arguments": {"id": "http://oeoa.test/jobs/job-b2_fapar/results"},
            },
            "mc1": {
                "process_id": "merge_cubes",
                "arguments": {
                    "cube1": {"from_node": "lc1"},
                    "cube2": {"from_node": "lc2"},
                },
            },
            "sr1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        assert aggregator.jobs["job-b2_fapar"].pg == {
            "lc2": {
                "process_id": "load_collection",
                "arguments": {"id": "B2_FAPAR"},
            },
            "sr1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                "result": True,
            },
        }
