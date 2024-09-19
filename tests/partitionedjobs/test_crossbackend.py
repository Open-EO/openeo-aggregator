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
    CrossBackendJobSplitter,
    DeepGraphSplitter,
    GraphSplitException,
    LoadCollectionGraphSplitter,
    SubGraphId,
    SupportingBackendsMapper,
    _GraphViewer,
    _GVNode,
    _PGSplitResult,
    _SubGraphData,
    run_partitioned_job,
)


class TestCrossBackendSplitter:
    def test_split_simple(self):
        process_graph = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: "foo")
        )
        res = splitter.split({"process_graph": process_graph})

        assert res.subjobs == {"main": SubJob(process_graph, backend_id=None)}
        assert res.dependencies == {}

    def test_split_streaming_simple(self):
        process_graph = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: "foo")
        )
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
            "_agg_crossbackend_save_result": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        )
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
                    "_agg_crossbackend_save_result": {
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
                    "_agg_crossbackend_save_result": {
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
            "_agg_crossbackend_save_result": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        )
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
                        "_agg_crossbackend_save_result": {
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
                        "_agg_crossbackend_save_result": {
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
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        )

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
                        "_agg_crossbackend_save_result": {
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
                        "_agg_crossbackend_save_result": {
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
            "_agg_crossbackend_save_result": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "mc1"}, "format": "NetCDF"},
                "result": True,
            },
        }
        splitter = CrossBackendJobSplitter(
            graph_splitter=LoadCollectionGraphSplitter(backend_for_collection=lambda cid: cid.split("_")[0])
        )
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
            "_agg_crossbackend_save_result": {
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
            "_agg_crossbackend_save_result": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc2"}, "format": "GTiff"},
                "result": True,
            },
        }


class TestGVNode:
    def test_defaults(self):
        node = _GVNode()
        assert isinstance(node.depends_on, frozenset)
        assert node.depends_on == frozenset()
        assert isinstance(node.flows_to, frozenset)
        assert node.flows_to == frozenset()
        assert node.backend_candidates is None

    def test_basic(self):
        node = _GVNode(depends_on=["a", "b"], flows_to=["c", "d"], backend_candidates=["X"])
        assert isinstance(node.depends_on, frozenset)
        assert node.depends_on == frozenset(["a", "b"])
        assert isinstance(node.flows_to, frozenset)
        assert node.flows_to == frozenset(["c", "d"])
        assert isinstance(node.backend_candidates, frozenset)
        assert node.backend_candidates == frozenset(["X"])

    def test_single_strings(self):
        node = _GVNode(depends_on="apple", flows_to="banana", backend_candidates="coconut")
        assert isinstance(node.depends_on, frozenset)
        assert node.depends_on == frozenset(["apple"])
        assert isinstance(node.flows_to, frozenset)
        assert node.flows_to == frozenset(["banana"])
        assert isinstance(node.backend_candidates, frozenset)
        assert node.backend_candidates == frozenset(["coconut"])

    def test_eq(self):
        assert _GVNode() == _GVNode()
        assert _GVNode(
            depends_on=["a", "b"],
            flows_to=["c", "d"],
            backend_candidates="X",
        ) == _GVNode(
            depends_on=("b", "a"),
            flows_to={"d", "c"},
            backend_candidates=["X"],
        )


def supporting_backends_from_node_id_dict(data: dict) -> SupportingBackendsMapper:
    return lambda node_id, node: data.get(node_id)


class TestGraphViewer:
    def test_empty(self):
        graph = _GraphViewer(node_map={})
        assert list(graph.iter_nodes()) == []

    @pytest.mark.parametrize(
        ["node_map", "expected_error"],
        [
            ({"a": _GVNode(flows_to="b")}, r"Inconsistent.*unknown=\{'b'\}"),
            ({"b": _GVNode(depends_on="a")}, r"Inconsistent.*unknown=\{'a'\}"),
            ({"a": _GVNode(flows_to="b"), "b": _GVNode()}, r"Inconsistent.*bad_links=\{\('a', 'b'\)\}"),
            ({"b": _GVNode(depends_on="a"), "a": _GVNode()}, r"Inconsistent.*bad_links=\{\('a', 'b'\)\}"),
        ],
    )
    def test_check_consistency(self, node_map, expected_error):
        with pytest.raises(GraphSplitException, match=expected_error):
            _ = _GraphViewer(node_map=node_map)

    def test_from_flat_graph_basic(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "B1_NDVI"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _GraphViewer.from_flat_graph(
            flat, supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"]})
        )
        assert sorted(graph.iter_nodes()) == [
            ("lc1", _GVNode(flows_to=["ndvi1"], backend_candidates="b1")),
            ("ndvi1", _GVNode(depends_on=["lc1"])),
        ]

    # TODO: test from_flat_graph with more complex graphs

    def test_from_edges(self):
        graph = _GraphViewer.from_edges([("a", "c"), ("b", "d"), ("c", "e"), ("d", "e"), ("e", "f")])
        assert sorted(graph.iter_nodes()) == [
            ("a", _GVNode(flows_to=["c"])),
            ("b", _GVNode(flows_to=["d"])),
            ("c", _GVNode(depends_on=["a"], flows_to=["e"])),
            ("d", _GVNode(depends_on=["b"], flows_to=["e"])),
            ("e", _GVNode(depends_on=["c", "d"], flows_to=["f"])),
            ("f", _GVNode(depends_on=["e"])),
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
            (["c", "a"], True, ["a", "c"]),
            (["c", "a"], False, []),
            (["e"], True, ["e", "c", "d", "a", "b"]),
            (["e"], False, ["c", "d", "a", "b"]),
            (["e", "d"], True, ["d", "e", "b", "c", "a"]),
            (["e", "d"], False, ["c", "b", "a"]),
            (["d", "e"], True, ["d", "e", "b", "c", "a"]),
            (["d", "e"], False, ["b", "c", "a"]),
            (["f", "c"], True, ["c", "f", "a", "e", "d", "b"]),
            (["f", "c"], False, ["e", "a", "d", "b"]),
        ],
    )
    def test_walk_upstream_nodes(self, seed, include_seeds, expected):
        graph = _GraphViewer.from_edges(
            # a   b
            # |   |
            # c   d
            #  \ /
            #   e
            #   |
            #   f
            [("a", "c"), ("b", "d"), ("c", "e"), ("d", "e"), ("e", "f")]
        )
        assert list(graph.walk_upstream_nodes(seed, include_seeds)) == expected

    def test_get_backend_candidates_basic(self):
        graph = _GraphViewer.from_edges(
            # a
            # |
            # b   c
            #  \ /
            #   d
            [("a", "b"), ("b", "d"), ("c", "d")],
            supporting_backends_mapper=supporting_backends_from_node_id_dict({"a": ["b1"], "c": ["b2"]}),
        )
        assert graph.get_backend_candidates_for_node("a") == {"b1"}
        assert graph.get_backend_candidates_for_node("b") == {"b1"}
        assert graph.get_backend_candidates_for_node("c") == {"b2"}
        assert graph.get_backend_candidates_for_node("d") == set()

        assert graph.get_backend_candidates_for_node_set(["a"]) == {"b1"}
        assert graph.get_backend_candidates_for_node_set(["b"]) == {"b1"}
        assert graph.get_backend_candidates_for_node_set(["c"]) == {"b2"}
        assert graph.get_backend_candidates_for_node_set(["d"]) == set()
        assert graph.get_backend_candidates_for_node_set(["a", "b"]) == {"b1"}
        assert graph.get_backend_candidates_for_node_set(["a", "b", "c"]) == set()
        assert graph.get_backend_candidates_for_node_set(["a", "b", "d"]) == set()

    def test_get_backend_candidates_none(self):
        graph = _GraphViewer.from_edges(
            # a
            # |
            # b   c
            #  \ /
            #   d
            [("a", "b"), ("b", "d"), ("c", "d")],
        )
        assert graph.get_backend_candidates_for_node("a") is None
        assert graph.get_backend_candidates_for_node("b") is None
        assert graph.get_backend_candidates_for_node("c") is None
        assert graph.get_backend_candidates_for_node("d") is None

        assert graph.get_backend_candidates_for_node_set(["a", "b"]) is None
        assert graph.get_backend_candidates_for_node_set(["a", "b", "c"]) is None

    def test_get_backend_candidates_intersection(self):
        graph = _GraphViewer.from_edges(
            # a   b   c
            #  \ / \ /
            #   d   e
            #    \ /
            #     f
            [("a", "d"), ("b", "d"), ("b", "e"), ("c", "e"), ("d", "f"), ("e", "f")],
            supporting_backends_mapper=supporting_backends_from_node_id_dict(
                {"a": ["b1", "b2"], "b": ["b2", "b3"], "c": ["b4"]}
            ),
        )
        assert graph.get_backend_candidates_for_node("a") == {"b1", "b2"}
        assert graph.get_backend_candidates_for_node("b") == {"b2", "b3"}
        assert graph.get_backend_candidates_for_node("c") == {"b4"}
        assert graph.get_backend_candidates_for_node("d") == {"b2"}
        assert graph.get_backend_candidates_for_node("e") == set()
        assert graph.get_backend_candidates_for_node("f") == set()

        assert graph.get_backend_candidates_for_node_set(["a", "b"]) == {"b2"}
        assert graph.get_backend_candidates_for_node_set(["a", "b", "d"]) == {"b2"}
        assert graph.get_backend_candidates_for_node_set(["c", "d"]) == set()

    def test_find_forsaken_nodes(self):
        graph = _GraphViewer.from_edges(
            # a   b   c
            #  \ / \ /
            #   d   e
            #    \ /
            #     f
            #    / \
            #   g   h
            [("a", "d"), ("b", "d"), ("b", "e"), ("c", "e"), ("d", "f"), ("e", "f"), ("f", "g"), ("f", "h")],
            supporting_backends_mapper=supporting_backends_from_node_id_dict(
                {"a": ["b1", "b2"], "b": ["b2", "b3"], "c": ["b4"]}
            ),
        )
        assert graph.find_forsaken_nodes() == {"e", "f", "g", "h"}

    def test_find_articulation_points_basic(self):
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _GraphViewer.from_flat_graph(flat)
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
        graph = _GraphViewer.from_flat_graph(flat)
        assert graph.find_articulation_points() == expected

    def test_split_at_minimal(self):
        graph = _GraphViewer.from_edges(
            [("a", "b")], supporting_backends_mapper=supporting_backends_from_node_id_dict({"a": "A"})
        )
        # Split at a
        up, down = graph.split_at("a")
        assert sorted(up.iter_nodes()) == [
            ("a", _GVNode(backend_candidates=["A"])),
        ]
        assert sorted(down.iter_nodes()) == [
            ("a", _GVNode(flows_to=["b"])),
            ("b", _GVNode(depends_on=["a"])),
        ]
        # Split at b
        up, down = graph.split_at("b")
        assert sorted(up.iter_nodes()) == [
            ("a", _GVNode(flows_to=["b"], backend_candidates=["A"])),
            ("b", _GVNode(depends_on=["a"])),
        ]
        assert sorted(down.iter_nodes()) == [
            ("b", _GVNode()),
        ]

    def test_split_at_basic(self):
        graph = _GraphViewer.from_edges(
            [("a", "b"), ("b", "c")],
            supporting_backends_mapper=supporting_backends_from_node_id_dict({"a": "A"}),
        )
        up, down = graph.split_at("b")
        assert sorted(up.iter_nodes()) == [
            ("a", _GVNode(flows_to=["b"], backend_candidates=["A"])),
            ("b", _GVNode(depends_on=["a"])),
        ]
        assert sorted(down.iter_nodes()) == [
            ("b", _GVNode(flows_to=["c"])),
            ("c", _GVNode(depends_on=["b"])),
        ]

    def test_split_at_complex(self):
        graph = _GraphViewer.from_edges(
            [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("c", "e"), ("e", "g"), ("f", "g"), ("X", "Y")]
        )
        up, down = graph.split_at("e")
        assert sorted(up.iter_nodes()) == sorted(
            _GraphViewer.from_edges([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("c", "e")]).iter_nodes()
        )
        assert sorted(down.iter_nodes()) == sorted(
            _GraphViewer.from_edges([("e", "g"), ("f", "g"), ("X", "Y")]).iter_nodes()
        )

    def test_split_at_non_articulation_point(self):
        graph = _GraphViewer.from_edges(
            #   a
            #  /|
            # b |
            #  \|
            #   c
            [("a", "b"), ("b", "c"), ("a", "c")]
        )

        with pytest.raises(GraphSplitException, match="not an articulation point"):
            _ = graph.split_at("b")

        # These should still work
        up, down = graph.split_at("a")
        assert sorted(up.iter_nodes()) == [
            ("a", _GVNode()),
        ]
        assert sorted(down.iter_nodes()) == [
            ("a", _GVNode(flows_to=["b", "c"])),
            ("b", _GVNode(depends_on=["a"], flows_to=["c"])),
            ("c", _GVNode(depends_on=["a", "b"])),
        ]

        up, down = graph.split_at("c")
        assert sorted(up.iter_nodes()) == [
            ("a", _GVNode(flows_to=["b", "c"])),
            ("b", _GVNode(depends_on=["a"], flows_to=["c"])),
            ("c", _GVNode(depends_on=["a", "b"])),
        ]
        assert sorted(down.iter_nodes()) == [
            ("c", _GVNode()),
        ]

    def test_produce_split_locations_simple(self):
        """Simple produce_split_locations use case: no need for splits"""
        flat = {
            # lc1
            #  |
            # ndvi1
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        graph = _GraphViewer.from_flat_graph(
            flat, supporting_backends=supporting_backends_from_node_id_dict({"lc1": "b1"})
        )
        assert list(graph.produce_split_locations()) == [[]]

    def test_produce_split_locations_merge_basic(self):
        """
        Basic produce_split_locations use case:
        two load collections on different backends and a merge
        """
        flat = {
            #  lc1   lc2
            #     \ /
            #    merge1
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
            },
        }
        graph = _GraphViewer.from_flat_graph(
            flat,
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]}),
        )
        assert sorted(graph.produce_split_locations()) == [["lc1"], ["lc2"]]

    def test_produce_split_locations_merge_longer(self):
        flat = {
            #   lc1     lc2
            #    |       |
            #  bands1  bands2
            #       \ /
            #      merge1
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "bands1": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc1"}, "bands": ["B01"]}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "bands2": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc2"}, "bands": ["B02"]}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "bands1"}, "cube2": {"from_node": "bands2"}},
            },
        }
        graph = _GraphViewer.from_flat_graph(
            flat,
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]}),
        )
        assert sorted(graph.produce_split_locations(limit=2)) == [["bands1"], ["bands2"]]
        assert list(graph.produce_split_locations(limit=4)) == [["bands1"], ["bands2"], ["lc1"], ["lc2"]]

    def test_produce_split_locations_merge_longer_triangle(self):
        flat = {
            #        lc1
            #       /  |
            #  bands1  |     lc2
            #      \   |      |
            #      mask1   bands2
            #          \  /
            #         merge1
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
        graph = _GraphViewer.from_flat_graph(
            flat,
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]}),
        )
        assert list(graph.produce_split_locations(limit=4)) == [["bands2"], ["mask1"], ["lc2"], ["lc1"]]


class TestDeepGraphSplitter:
    def test_simple_no_split(self):
        splitter = DeepGraphSplitter(supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"]}))
        flat = {
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "lc1"}}, "result": True},
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"lc1", "ndvi1"},
            primary_backend_id="b1",
            secondary_graphs=[],
        )

    def test_simple_split(self):
        """
        Most simple split use case: two load_collections from different backends, merged.
        """
        splitter = DeepGraphSplitter(
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]})
        )
        flat = {
            #   lc1   lc2
            #      \ /
            #     merge
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "merge": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
                "result": True,
            },
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"lc1", "lc2", "merge"},
            primary_backend_id="b2",
            secondary_graphs=[
                _SubGraphData(
                    split_node="lc1",
                    node_ids={"lc1"},
                    backend_id="b1",
                )
            ],
        )

    def test_simple_deep_split(self):
        """
        Simple deep split use case:
        two load_collections from different backends, with some additional filtering, merged.
        """
        splitter = DeepGraphSplitter(
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]})
        )
        flat = {
            #   lc1      lc2
            #    |        |
            #  bands1  temporal2
            #       \  /
            #       merge
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "bands1": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc1"}, "bands": ["B01"]}},
            "temporal2": {
                "process_id": "filter_temporal",
                "arguments": {"data": {"from_node": "lc2"}, "extent": ["2022", "2023"]},
            },
            "merge": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "bands1"}, "cube2": {"from_node": "temporal2"}},
                "result": True,
            },
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"lc2", "temporal2", "bands1", "merge"},
            primary_backend_id="b2",
            secondary_graphs=[_SubGraphData(split_node="bands1", node_ids={"lc1", "bands1"}, backend_id="b1")],
        )

    def test_shallow_triple_split(self):
        splitter = DeepGraphSplitter(
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"], "lc3": ["b3"]})
        )
        flat = {
            #   lc1   lc2   lc3
            #      \ /      /
            #     merge1   /
            #        \    /
            #        merge2
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "lc3": {"process_id": "load_collection", "arguments": {"id": "S3"}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
            },
            "merge2": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "merge1"}, "cube2": {"from_node": "lc3"}},
                "result": True,
            },
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"lc1", "lc2", "lc3", "merge1", "merge2"},
            primary_backend_id="b2",
            secondary_graphs=[
                _SubGraphData(split_node="lc1", node_ids={"lc1"}, backend_id="b1"),
                _SubGraphData(split_node="lc3", node_ids={"lc3"}, backend_id="b3"),
            ],
        )

    def test_triple_split(self):
        splitter = DeepGraphSplitter(
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"], "lc3": ["b3"]})
        )
        flat = {
            #   lc1      lc2        lc3
            #    |        |          |
            #  bands1  temporal2  spatial3
            #       \  /          /
            #       merge1       /
            #            \      /
            #             merge2
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "lc3": {"process_id": "load_collection", "arguments": {"id": "S3"}},
            "bands1": {"process_id": "filter_bands", "arguments": {"data": {"from_node": "lc1"}, "bands": ["B01"]}},
            "temporal2": {
                "process_id": "filter_temporal",
                "arguments": {"data": {"from_node": "lc2"}, "extent": ["2022", "2023"]},
            },
            "spatial3": {"process_id": "filter_spatial", "arguments": {"data": {"from_node": "lc3"}, "extent": "EU"}},
            "merge1": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "bands1"}, "cube2": {"from_node": "temporal2"}},
            },
            "merge2": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "merge1"}, "cube2": {"from_node": "spatial3"}},
                "result": True,
            },
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"merge2", "merge1", "lc3", "spatial3"},
            primary_backend_id="b3",
            secondary_graphs=[
                _SubGraphData(split_node="bands1", node_ids={"bands1", "lc1"}, backend_id="b1"),
                _SubGraphData(split_node="merge1", node_ids={"bands1", "merge1", "temporal2", "lc2"}, backend_id="b2"),
            ],
        )

    @pytest.mark.parametrize(
        ["primary_backend", "secondary_graph"],
        [
            ("b1", _SubGraphData(split_node="lc2", node_ids={"lc2"}, backend_id="b2")),
            ("b2", _SubGraphData(split_node="lc1", node_ids={"lc1"}, backend_id="b1")),
        ],
    )
    def test_split_with_primary_backend(self, primary_backend, secondary_graph):
        """Test `primary_backend` argument of DeepGraphSplitter"""
        splitter = DeepGraphSplitter(
            supporting_backends=supporting_backends_from_node_id_dict({"lc1": ["b1"], "lc2": ["b2"]}),
            primary_backend=primary_backend,
        )
        flat = {
            #   lc1   lc2
            #      \ /
            #     merge
            "lc1": {"process_id": "load_collection", "arguments": {"id": "S1"}},
            "lc2": {"process_id": "load_collection", "arguments": {"id": "S2"}},
            "merge": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
                "result": True,
            },
        }
        result = splitter.split(flat)
        assert result == _PGSplitResult(
            primary_node_ids={"lc1", "lc2", "merge"},
            primary_backend_id=primary_backend,
            secondary_graphs=[secondary_graph],
        )
