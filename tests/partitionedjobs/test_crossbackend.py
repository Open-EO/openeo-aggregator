import dataclasses
import re
import types
from typing import Dict, List, Optional
from unittest import mock

import dirty_equals
import openeo
import pytest
import requests
import requests_mock
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet

from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.partitionedjobs.crossbackend import (
    CrossBackendSplitter,
    GraphSplitException,
    SubGraphId,
    _FrozenGraph,
    _FrozenNode,
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


class TestFrozenGraph:
    def test_empty(self):
        graph = _FrozenGraph(graph={})
        assert list(graph.iter_nodes()) == []

    def test_from_flat_graph_basic(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={"lc1": ["b1"]})
        assert sorted(graph.iter_nodes()) == [
            (
                "lc1",
                _FrozenNode(frozenset(), frozenset(["ndvi1"]), backend_candidates=frozenset(["b1"])),
            ),
            ("ndvi1", _FrozenNode(frozenset(["lc1"]), frozenset([]), backend_candidates=None)),
        ]

    # TODO: test from_flat_graph with more complex graphs

    def test_from_edges(self):
        graph = _FrozenGraph.from_edges([("a", "c"), ("b", "d"), ("c", "e"), ("d", "e"), ("e", "f")])
        assert sorted(graph.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["c"]), backend_candidates=None)),
            ("b", _FrozenNode(frozenset(), frozenset(["d"]), backend_candidates=None)),
            ("c", _FrozenNode(frozenset(["a"]), frozenset(["e"]), backend_candidates=None)),
            ("d", _FrozenNode(frozenset(["b"]), frozenset(["e"]), backend_candidates=None)),
            ("e", _FrozenNode(frozenset(["c", "d"]), frozenset("f"), backend_candidates=None)),
            ("f", _FrozenNode(frozenset(["e"]), frozenset(), backend_candidates=None)),
        ]

    @pytest.mark.parametrize(
        ["seed", "include_seeds", "expected"],
        [
            (["a"], True, ["a"]),
            (["a"], False, []),
            (["c"], True, ["c", "a"]),
            (["c"], False, ["a"]),
            (["a", "c"], True, ["a", "c"]),
            (["a", "c"], False, []),
            (["c", "a"], True, ["c", "a"]),
            (["c", "a"], False, []),
            (
                ["e"],
                True,
                dirty_equals.IsOneOf(
                    ["e", "c", "d", "a", "b"],
                    ["e", "d", "c", "b", "a"],
                ),
            ),
            (
                ["e"],
                False,
                dirty_equals.IsOneOf(
                    ["c", "d", "a", "b"],
                    ["d", "c", "b", "a"],
                ),
            ),
            (["e", "d"], True, ["e", "d", "c", "b", "a"]),
            (["e", "d"], False, ["c", "b", "a"]),
            (["d", "e"], True, ["d", "e", "b", "c", "a"]),
            (["d", "e"], False, ["b", "c", "a"]),
            (["f", "c"], True, ["f", "c", "e", "a", "d", "b"]),
            (["f", "c"], False, ["e", "a", "d", "b"]),
        ],
    )
    def test_walk_upstream_nodes(self, seed, include_seeds, expected):
        graph = _FrozenGraph.from_edges([("a", "c"), ("b", "d"), ("c", "e"), ("d", "e"), ("e", "f")])
        assert list(graph.walk_upstream_nodes(seed, include_seeds)) == expected

    def test_get_backend_candidates_basic(self):
        graph = _FrozenGraph.from_edges(
            [("a", "b"), ("b", "d"), ("c", "d")],
            backend_candidates_map={"a": ["b1"], "c": ["b2"]},
        )
        assert graph.get_backend_candidates("a") == {"b1"}
        assert graph.get_backend_candidates("b") == {"b1"}
        assert graph.get_backend_candidates("c") == {"b2"}
        assert graph.get_backend_candidates("d") == set()

    def test_get_backend_candidates_none(self):
        graph = _FrozenGraph.from_edges(
            [("a", "b"), ("b", "d"), ("c", "d")],
            backend_candidates_map={},
        )
        assert graph.get_backend_candidates("a") is None
        assert graph.get_backend_candidates("b") is None
        assert graph.get_backend_candidates("c") is None
        assert graph.get_backend_candidates("d") is None

    def test_get_backend_candidates_intersection(self):
        graph = _FrozenGraph.from_edges(
            [("a", "d"), ("b", "d"), ("b", "e"), ("c", "e"), ("d", "f"), ("e", "f")],
            backend_candidates_map={"a": ["b1", "b2"], "b": ["b2", "b3"], "c": ["b4"]},
        )
        assert graph.get_backend_candidates("a") == {"b1", "b2"}
        assert graph.get_backend_candidates("b") == {"b2", "b3"}
        assert graph.get_backend_candidates("c") == {"b4"}
        assert graph.get_backend_candidates("d") == {"b2"}
        assert graph.get_backend_candidates("e") == set()
        assert graph.get_backend_candidates("f") == set()

    def test_find_forsaken_nodes(self):
        graph = _FrozenGraph.from_edges(
            [("a", "d"), ("b", "d"), ("b", "e"), ("c", "e"), ("d", "f"), ("e", "f"), ("f", "g"), ("f", "h")],
            backend_candidates_map={"a": ["b1", "b2"], "b": ["b2", "b3"], "c": ["b4"]},
        )
        assert graph.find_forsaken_nodes() == {"e", "f", "g", "h"}

    def test_find_articulation_points_basic(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={})
        assert graph.find_articulation_points() == {"lc1", "ndvi1"}

    @pytest.mark.parametrize(
        ["flat", "expected"],
        [
            (
                {
                    "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
                    "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}},
                },
                {"lc1", "ndvi1"},
            ),
            (
                {
                    "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
                    "bands1": {
                        "process_id": "filter_bands",
                        "arguments": {"data": {"from_node": "lc1"}, "bands": ["b1"]},
                    },
                    "bands2": {
                        "process_id": "filter_bands",
                        "arguments": {"data": {"from_node": "lc1"}, "bands": ["b2"]},
                    },
                    "merge1": {
                        "process_id": "merge_cubes",
                        "arguments": {"cube1": {"from_node": "bands1"}, "cube2": {"from_node": "bands2"}},
                    },
                    "save1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "merge1"}, "format": "GTiff"},
                    },
                },
                {"lc1", "merge1", "save1"},
            ),
            (
                {
                    "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
                    "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
                    "merge1": {
                        "process_id": "merge_cubes",
                        "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
                    },
                    "save1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "merge1"}, "format": "GTiff"},
                    },
                },
                {"lc1", "lc2", "merge1", "save1"},
            ),
            (
                {
                    "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
                    "bands1": {
                        "process_id": "filter_bands",
                        "arguments": {"data": {"from_node": "lc1"}, "bands": ["b1"]},
                    },
                    "bbox1": {
                        "process_id": "filter_spatial",
                        "arguments": {"data": {"from_node": "bands1"}},
                    },
                    "merge1": {
                        "process_id": "merge_cubes",
                        "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "bbox1"}},
                    },
                    "save1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "merge1"}},
                    },
                },
                {"lc1", "merge1", "save1"},
            ),
        ],
    )
    def test_find_articulation_points(self, flat, expected):
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={})
        assert graph.find_articulation_points() == expected

    def test_split_at_minimal(self):
        graph = _FrozenGraph.from_edges([("a", "b")], backend_candidates_map={"a": "A"})
        # Split at a
        down, up = graph.split_at("a")
        assert sorted(up.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(), backend_candidates=frozenset(["A"]))),
        ]
        assert sorted(down.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["b"]), backend_candidates=None)),
            ("b", _FrozenNode(frozenset(["a"]), frozenset([]), backend_candidates=None)),
        ]
        # Split at b
        down, up = graph.split_at("b")
        assert sorted(up.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["b"]), backend_candidates=frozenset(["A"]))),
            ("b", _FrozenNode(frozenset(["a"]), frozenset([]), backend_candidates=None)),
        ]
        assert sorted(down.iter_nodes()) == [
            ("b", _FrozenNode(frozenset(), frozenset(), backend_candidates=None)),
        ]

    def test_split_at_basic(self):
        graph = _FrozenGraph.from_edges([("a", "b"), ("b", "c")], backend_candidates_map={"a": "A"})
        down, up = graph.split_at("b")
        assert sorted(up.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["b"]), backend_candidates=frozenset(["A"]))),
            ("b", _FrozenNode(frozenset(["a"]), frozenset([]), backend_candidates=None)),
        ]
        assert sorted(down.iter_nodes()) == [
            ("b", _FrozenNode(frozenset(), frozenset(["c"]), backend_candidates=None)),
            ("c", _FrozenNode(frozenset(["b"]), frozenset([]), backend_candidates=None)),
        ]

    def test_split_at_complex(self):
        graph = _FrozenGraph.from_edges(
            [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("c", "e"), ("e", "g"), ("f", "g"), ("X", "Y")]
        )
        down, up = graph.split_at("e")
        assert sorted(up.iter_nodes()) == sorted(
            _FrozenGraph.from_edges([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("c", "e")]).iter_nodes()
        )
        assert sorted(down.iter_nodes()) == sorted(
            _FrozenGraph.from_edges([("e", "g"), ("f", "g"), ("X", "Y")]).iter_nodes()
        )

    def test_split_at_non_articulation_point(self):
        graph = _FrozenGraph.from_edges([("a", "b"), ("b", "c"), ("a", "c")])
        with pytest.raises(GraphSplitException, match="not an articulation point"):
            _ = graph.split_at("b")

        # These should still work
        down, up = graph.split_at("a")
        assert sorted(up.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(), backend_candidates=None)),
        ]
        assert sorted(down.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["b", "c"]), backend_candidates=None)),
            ("b", _FrozenNode(frozenset(["a"]), frozenset(["c"]), backend_candidates=None)),
            ("c", _FrozenNode(frozenset(["a", "b"]), frozenset(), backend_candidates=None)),
        ]

        down, up = graph.split_at("c")
        assert sorted(up.iter_nodes()) == [
            ("a", _FrozenNode(frozenset(), frozenset(["b", "c"]), backend_candidates=None)),
            ("b", _FrozenNode(frozenset(["a"]), frozenset(["c"]), backend_candidates=None)),
            ("c", _FrozenNode(frozenset(["a", "b"]), frozenset(), backend_candidates=None)),
        ]
        assert sorted(down.iter_nodes()) == [
            ("c", _FrozenNode(frozenset(), frozenset(), backend_candidates=None)),
        ]

    def test_produce_split_locations_simple(self):
        """Simple produce_split_locations use case: no need for splits"""
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={"lc1": "b1"})
        assert list(graph.produce_split_locations()) == [[]]

    def test_produce_split_locations_merge_basic(self):
        """
        Basic produce_split_locations use case:
        two load collections on different backends and a merge
        """
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
            },
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={"lc1": ["b1"], "lc2": ["b2"]})
        assert sorted(graph.produce_split_locations()) == [["lc1"], ["lc2"]]

    def test_produce_split_locations_merge_longer(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "bands1": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc1"}, "bands": ["B01"]}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "bands2": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc2"}, "bands": ["B02"]}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "bands1"}, "cube2": {"from_node": "bands2"}},
            },
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={"lc1": ["b1"], "lc2": ["b2"]})
        assert sorted(graph.produce_split_locations(limit=2)) == [["bands1"], ["bands2"]]
        assert list(graph.produce_split_locations(limit=4)) == dirty_equals.IsOneOf(
            [["bands1"], ["bands2"], ["lc1"], ["lc2"]],
            [["bands2"], ["bands1"], ["lc2"], ["lc1"]],
        )

    def test_produce_split_locations_merge_longer_triangle(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "bands1": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc1"}, "bands": ["B01"]}},
            "mask1": {
                "process_id": "mask",
                "arguments": {"data": {"from_node": "bands1"}, "mask": {"from_node": "lc1"}},
            },
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "bands2": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc2"}, "bands": ["B02"]}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "mask1"}, "cube2": {"from_node": "bands2"}},
            },
        }
        graph = _FrozenGraph.from_flat_graph(flat, backend_candidates_map={"lc1": ["b1"], "lc2": ["b2"]})
        assert list(graph.produce_split_locations(limit=4)) == dirty_equals.IsOneOf(
            [["mask1"], ["bands2"], ["lc1"], ["lc2"]],
            [["bands2"], ["mask1"], ["lc2"], ["lc1"]],
        )
