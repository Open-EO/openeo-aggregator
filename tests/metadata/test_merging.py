import pytest

from openeo_aggregator.metadata.merging import ProcessMetadataMerger


class ListReporter:
    """Reporter for testing"""

    def __init__(self):
        self.logs = []

    def __call__(self, *args, **kwargs):
        self.logs.append((args, kwargs))


@pytest.fixture
def reporter() -> ListReporter:
    return ListReporter()


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
        }
        assert reporter.logs == []

    def test_merge_process_returns(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {"id": "add", "returns": {"schema": {"type": "number"}}},
                "b2": {"id": "add", "returns": {"schema": {"type": "number"}}},
            }
        )

        assert result == {
            "id": "add",
            "description": "add",
            "parameters": [],
            "returns": {"schema": {"type": "number"}},
            "federation:backends": ["b1", "b2"],
        }
        assert reporter.logs == []

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
        }
        assert reporter.logs == []

    def test_merge_process_categories(self, merger, reporter):
        result = merger.merge_process_metadata(
            {
                "b1": {
                    "id": "add",
                    "categories": ["Math"],
                },
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
        }
        assert reporter.logs == []

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
        }
        assert reporter.logs == []

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
        }
        assert reporter.logs == [
            (
                (
                    "Parameter 'x' field 'schema' value {'type': 'array'} differs from merged {'type': 'number'}",
                ),
                {"backend_id": "b3", "process_id": "cos"},
            )
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
        }

        assert reporter.logs == []
