import pytest

from openeo_aggregator.metadata.merging import ProcessMetadataMerger, json_diff
from openeo_aggregator.testing import same_repr
from openeo_driver.testing import DictSubSet


class TestMergeProcessMetadata:
    @pytest.fixture
    def merger(self, reporter) -> ProcessMetadataMerger:
        return ProcessMetadataMerger(report=reporter)

    def test_merge_processes_empty(self):
        assert ProcessMetadataMerger().merge_processes_metadata({}) == {}

    def test_merge_processes_minimal(self, merger, reporter):
        result = merger.merge_processes_metadata(
            {
                "b1": {"add": {"id": "add"}},
                "b2": {"add": {"id": "add"}},
            }
        )

        assert result == {
            "add": {
                "id": "add",
                "description": "add",
                "parameters": [],
                "returns": {"schema": {}},
                "federation:backends": ["b1", "b2"],
                'deprecated': False,
                'experimental': False,
                'examples': [],
                'links': [],
            }
        }
        assert reporter.logs == []

    def test_merge_process_minimal(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add"},
                "b2": {"id": "add"},
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {}},
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == []

    def test_merge_process_returns(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "add",
                    "returns": {
                        "schema": {
                            "type": "array",
                            "items": {
                                "description": "All data type are allowed."
                            }
                        },
                        "description": "some description"
                    }
                },
                "b2": {
                    "id": "add",
                    "returns": {
                        "schema": {
                            "type": "array",
                            "items": {
                                "description": "Any data type is allowed."
                            }
                        }
                    }
                },
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            'returns': {
                'schema': {
                    'type': 'array',
                    'items': {}
                }
            },
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == []

    def test_merge_process_returns_difference(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", "returns": {"schema": {"type": "number"}}},
                "b2": {"id": "add", "returns": {"schema": {"type": "array"}}},
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {"type": "number"}},
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == [
            {
                "msg": "Returns schema is different from merged.",
                "backend_id": "b2",
                "process_id": "add",
                "merged": {"schema": {"type": "number"}},
                "value": {"schema": {"type": "array"}},
                "diff": [
                    "--- merged\n",
                    "+++ b2\n",
                    "@@ -1,5 +1,5 @@\n",
                    " {\n",
                    '   "schema": {\n',
                    '-    "type": "number"\n',
                    '+    "type": "array"\n',
                    "   }\n",
                    " }\n",
                ],
            }
        ]

    def test_merge_process_exceptions(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "add",
                    "exceptions": {"MathError": {"message": "Nope"}},
                },
                "b2": {
                    "id": "add",
                    "exceptions": {
                        "MathError": {"message": "Nope"},
                        "OverflowError": {"message": "Jeez"},
                    },
                },
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {}},
            "exceptions": {
                "MathError": {"message": "Nope"},
                "OverflowError": {"message": "Jeez"},
            },
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == []

    def test_merge_process_exceptions_invalid(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "add",
                    "exceptions": {"MathError": {"message": "Nope"}},
                },
                "b2": {
                    "id": "add",
                    "exceptions": 1234,
                },
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {}},
            "exceptions": {"MathError": {"message": "Nope"}},
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == [
            {
                "msg": "Invalid process exceptions listing",
                "process_id": "add",
                "backend_id": "b2",
                "exceptions": 1234,
            }
        ]

    def test_merge_process_categories(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", "categories": ["Math"]},
                "b2": {"id": "add", "categories": ["Math", "Maths"]},
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {}},
            "categories": ["Math", "Maths"],
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == []

    def test_merge_process_categories_invalid(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", "categories": ["Math"]},
                "b2": {"id": "add", "categories": "Maths"},
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {}},
            "categories": ["Math"],
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == [
            {
                "msg": "Invalid process categories listing.",
                "backend_id": "b2",
                "process_id": "add",
                "categories": "Maths",
            }
        ]

    def test_merge_process_parameters_basic(self, merger, reporter):
        spec = {
            "id": "add",
            "parameters": [
                {"name": "x", "schema": {"type": "number"}},
                {"name": "y", "schema": {"type": "number"}},
            ],
        }
        result = merger.merge_process_metadata(
            {
                "b1": spec,
                "b2": spec,
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [
                {"name": "x", "description": "x", "schema": {"type": "number"}},
                {"name": "y", "description": "y", "schema": {"type": "number"}},
            ],
            "returns": {"schema": {}},
            "federation:backends": ["b1", "b2"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == []

    def test_merge_process_parameters_invalid(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "cos",
                    "parameters": [{"name": "x", "schema": {"type": "number"}}],
                },
                "b2": {"id": "cos", "parameters": [1324]},
            }
        )

        assert result == {
            "description": "cos",
            "federation:backends": ["b1", "b2"],
            "id": "cos",
            "parameters": [
                {"description": "x", "name": "x", "schema": {"type": "number"}}
            ],
            "returns": {"schema": {}},
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': []
        }
        assert reporter.logs == [
            {
                "msg": "Invalid parameter metadata",
                "backend_id": "b2",
                "process_id": "cos",
                "exception": same_repr(TypeError("'int' object is not subscriptable")),
                "parameter": 1324,
            },
            {
                "msg": "Missing parameters.",
                "backend_id": "b2",
                "process_id": "cos",
                "missing_parameters": {"x"},
            },
        ]

    def test_merge_process_parameters_invalid_listing(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "cos",
                    "parameters": [{"name": "x", "schema": {"type": "number"}}],
                },
                "b2": {"id": "cos", "parameters": 1324},
            }
        )

        assert result == {
            "description": "cos",
            "federation:backends": ["b1", "b2"],
            "id": "cos",
            "parameters": [
                {"description": "x", "name": "x", "schema": {"type": "number"}}
            ],
            "returns": {"schema": {}},
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': [],
        }
        assert reporter.logs == [
            {
                "msg": "Invalid parameter listing",
                "backend_id": "b2",
                "process_id": "cos",
                "exception": same_repr(TypeError("'int' object is not iterable")),
                "parameters": 1324,
            },
            {
                "msg": "Missing parameters.",
                "backend_id": "b2",
                "process_id": "cos",
                "missing_parameters": {"x"},
            },
        ]

    def test_merge_process_parameters_differences(self, merger, reporter):
        by_backend = {
            "b1": {
                "id": "cos",
                "parameters": [
                    {"name": "x", "schema": {"type": "number"}},
                ],
            },
            "b2": {
                "id": "cos",
                "parameters": [
                    {
                        "name": "x",
                        "description": "X value",
                        "schema": {"type": "number"},
                        "deprecated": False,
                        "optional": False,
                    },
                ],
            },
            "b3": {
                "id": "cos",
                "parameters": [
                    {"name": "x", "schema": {"type": "array"}},
                ],
            },
        }
        merged = merger.merge_process_metadata(by_backend=by_backend)
        assert merged == {
            "id": "cos",
            "description": "cos",
            "parameters": [
                {"description": "x", "name": "x", "schema": {"type": "number"}}
            ],
            "returns": {"schema": {}},
            "federation:backends": ["b1", "b2", "b3"],
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': [],
        }
        assert reporter.logs == [
            {
                "msg": "Parameter 'x' field 'schema' value differs from merged.",
                "backend_id": "b3",
                "process_id": "cos",
                "merged": {"type": "number"},
                "value": {"type": "array"},
                "diff": [
                    "--- merged\n",
                    "+++ b3\n",
                    "@@ -1,3 +1,3 @@\n",
                    " {\n",
                    '-  "type": "number"\n',
                    '+  "type": "array"\n',
                    " }\n",
                ],
            }
        ]

    def test_merge_process_parameters_recursive(self, merger, reporter):
        """
        Comparison use case with sub-process-graph with parameters
        and some to-be-ignored differences in optional values.
        """
        by_backend = {
            "b1": {
                "id": "array_apply",
                "parameters": [
                    {
                        "name": "process",
                        "schema": {
                            "type": "object",
                            "subtype": "process-graph",
                            "parameters": [{"name": "x", "schema": {}}],
                            "returns": {"schema": {}},
                        },
                    },
                ],
            },
            "b2": {
                "id": "array_apply",
                "parameters": [
                    {
                        "name": "process",
                        "schema": {
                            "type": "object",
                            "subtype": "process-graph",
                            "parameters": [
                                {
                                    "name": "x",
                                    "schema": {},
                                    "optional": False,
                                    "deprecated": False,
                                    "experimental": False,
                                }
                            ],
                            "returns": {"schema": {}},
                        },
                        "optional": False,
                        "deprecated": False,
                        "experimental": False,
                    },
                ],
            },
        }
        merged = merger.merge_process_metadata(by_backend=by_backend)
        assert merged == {
            "id": "array_apply",
            "description": "array_apply",
            "parameters": [
                {
                    "name": "process",
                    "description": "process",
                    "schema": {
                        "type": "object",
                        "subtype": "process-graph",
                        "parameters": [{"name": "x", "description": "x", "schema": {}}],
                        "returns": {"schema": {}},
                    },
                }
            ],
            "returns": {"schema": {}},
            "experimental": False,
            "deprecated": False,
            "links": [],
            "examples": [],
            "federation:backends": ["b1", "b2"],
        }
        assert reporter.logs == []

    def test_merge_process_parameters_recursive2(self, merger, reporter):
        """
        Comparison use case with sub-process-graph with parameters
        and some to-be-ignored differences in optional values.
        """
        by_backend = {
            "b1": {
                "id": "count",
                "parameters": [
                    {"name": "data", "schema": {"type": "array"}},
                    {
                        "name": "condition",
                        "schema": [
                            {
                                "parameters": [{"name": "x", "schema": {}}],
                                "returns": {"schema": {"type": "boolean"}},
                                "subtype": "process-graph",
                                "type": "object",
                            },
                            {"const": True, "type": "boolean"},
                        ],
                        "default": None,
                        "optional": True,
                    },
                ],
                "returns": {"schema": {"type": "number"}},
            },
            "b2": {
                "id": "count",
                "deprecated": False,
                "exceptions": {},
                "experimental": False,
                "parameters": [
                    {
                        "name": "data",
                        "deprecated": False,
                        "description": "An array with elements of any data type.",
                        "experimental": False,
                        "optional": False,
                        "schema": {"type": "array"},
                    },
                    {
                        "name": "condition",
                        "deprecated": False,
                        "experimental": False,
                        "optional": True,
                        "schema": [
                            {
                                "parameters": [
                                    {
                                        "name": "x",
                                        "deprecated": False,
                                        "description": "The value of the current element being processed.",
                                        "experimental": False,
                                        "optional": False,
                                        "schema": {},
                                    },
                                ],
                                "returns": {"schema": {"type": "boolean"}},
                                "subtype": "process-graph",
                                "type": "object",
                            },
                            {"const": True, "type": "boolean"},
                        ],
                    },
                ],
                "returns": {"schema": {"type": "number"}},
                "summary": "Count the number of elements",
            },
        }
        merged = merger.merge_process_metadata(by_backend=by_backend)
        assert merged == {
            "id": "count",
            "description": "count",
            "federation:backends": ["b1", "b2"],
            "parameters": [
                {"name": "data", "description": "data", "schema": {"type": "array"}},
                {
                    "name": "condition",
                    "description": "condition",
                    "default": None,
                    "optional": True,
                    "schema": [
                        {
                            "type": "object",
                            "subtype": "process-graph",
                            "parameters": [
                                {"name": "x", "description": "x", "schema": {}}
                            ],
                            "returns": {"schema": {"type": "boolean"}},
                        },
                        {"const": True, "type": "boolean"},
                    ],
                },
            ],
            "returns": {"schema": {"type": "number"}},
            "summary": "Count the number of elements",
            'deprecated': False,
            'experimental': False,
            'examples': [],
            'links': [],
        }

        assert reporter.logs == []

    @pytest.mark.parametrize(
        ["flag", "v1", "v2", "expected"],
        [
            ("experimental", False, False, False),
            ("experimental", False, True, True),
            ("deprecated", False, False, False),
            ("deprecated", True, False, True),
        ],
    )
    def test_merge_process_flags(self, merger, reporter, flag, v1, v2, expected):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", flag: v1},
                "b2": {"id": "add", flag: v2},
            }
        )
        assert result == DictSubSet({"id": "add", flag: expected})

    def test_merge_process_examples(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "add",
                    "examples": [{"arguments": {"x": 3, "y": 5}, "returns": 8}],
                },
                "b2": {
                    "id": "add",
                    "examples": [{"arguments": {"x": 1, "y": 1}, "returns": 2}],
                },
            }
        )
        assert result == DictSubSet(
            {
                "id": "add",
                "examples": [
                    {"arguments": {"x": 3, "y": 5}, "returns": 8},
                    {"arguments": {"x": 1, "y": 1}, "returns": 2},
                ],
            }
        )

    def test_merge_process_links(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", "links": [{"rel": "about", "href": "/a"}]},
                "b2": {"id": "add", "links": [{"rel": "about", "href": "/b"}]},
            }
        )
        assert result == DictSubSet(
            {
                "id": "add",
                "links": [
                    {"rel": "about", "href": "/a"},
                    {"rel": "about", "href": "/b"},
                ],
            }
        )


def test_json_diff_empty():
    assert json_diff([1, 2], [1, 2]) == []


def test_json_diff_simple_list_difference():
    assert json_diff(
        [1, 2, 3, 4, 5, 6, 7],
        [1, 2, 3, 4444, 5, 6, 7],
        a_name="orig",
        b_name="changed",
        context=1,
    ) == [
        "--- orig\n",
        "+++ changed\n",
        "@@ -4,3 +4,3 @@\n",
        "   3,\n",
        "-  4,\n",
        "+  4444,\n",
        "   5,\n",
    ]


def test_json_diff_dict_sorting():
    diff = json_diff(
        {
            "foo": 1,
            "Bar": 2,
            "meh": [3, 4, 5],
            "lala": "haha",
            "xev": [{1: 2, 3: 4, 5: 6}],
        },
        {
            "meh": [3, 4, 5],
            "foo": 1,
            "xev": [{5: 6, 3: 4, 1: 2}],
            "lala": "haha",
            "Bar": 2,
        },
    )
    assert diff == []


def test_json_diff_dict_difference():
    diff = json_diff(
        {
            "foo": 1,
            "Bar": 2,
            "meh": [3, 4, 5],
            "lala": "haha",
            "xev": [{1: 2, 3: 4, 5: 6}],
        },
        {
            "meh": [3, 4, 5],
            "foo": 1,
            "xev": [{5: 6, 3: 4444, 1: 2}],
            "lalalalala": "haha",
            "Bar": 2,
        },
        a_name="orig",
        b_name="changed",
        context=2,
    )
    assert diff == [
        "--- orig\n",
        "+++ changed\n",
        "@@ -2,5 +2,5 @@\n",
        '   "Bar": 2,\n',
        '   "foo": 1,\n',
        '-  "lala": "haha",\n',
        '+  "lalalalala": "haha",\n',
        '   "meh": [\n',
        "     3,\n",
        "@@ -11,5 +11,5 @@\n",
        "     {\n",
        '       "1": 2,\n',
        '-      "3": 4,\n',
        '+      "3": 4444,\n',
        '       "5": 6\n',
        "     }\n",
    ]


def test_json_diff_scalar_difference():
    assert json_diff(
        "string",
        None,
        a_name="orig",
        b_name="changed",
        context=1,
    ) == ["--- orig\n", "+++ changed\n", "@@ -1 +1 @@\n", '-"string"\n', "+null\n"]
