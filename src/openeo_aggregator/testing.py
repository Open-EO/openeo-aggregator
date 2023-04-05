import datetime
import itertools
import json
import pathlib
import time
from typing import Any, List, Optional, Union
from unittest import mock

import kazoo
import kazoo.exceptions
import pytest
from openeo.util import rfc3339

import openeo_aggregator.about
from openeo_aggregator.utils import Clock


class DummyKazooClient:
    """
    Stand-in object for KazooClient for testing.
    """

    def __init__(self):
        self.state = "closed"
        self.data = {}

    def start(self):
        assert self.state == "closed"
        self.state = "open"

    def stop(self):
        assert self.state == "open"
        self.state = "closed"

    def create(self, path: str, value, makepath: bool = False):
        if path in self.data:
            raise kazoo.exceptions.NodeExistsError()
        parent = str(pathlib.Path(path).parent)
        if parent not in self.data and parent != path:
            if makepath:
                self.create(parent, b"", makepath=makepath)
            else:
                raise kazoo.exceptions.NoNodeError
        self.data[path] = value

    def exists(self, path):
        return path in self.data

    def get(self, path):
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        return (self.data[path], None)

    def get_children(self, path):
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        parent = path.split("/")
        return [p.split("/")[-1] for p in self.data if p.split("/")[:-1] == parent]

    def set(self, path, value, version=-1):
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        self.data[path] = value

    def get_data_deserialized(self, drop_empty=False) -> dict:
        """Dump whole db as a dict, but deserialize values"""

        def deserialize(b: bytes):
            if b[:2] == b'{"' and b[-1:] == b"}":
                return json.loads(b.decode("utf8"))
            else:
                return b.decode("utf8")

        return {
            k: deserialize(v)
            for (k, v) in self.data.items()
            if v or not drop_empty
        }


def approx_now(abs=10):
    """Pytest checker for whether timestamp is approximately 'now' (within some tolerance)."""
    return pytest.approx(Clock.time(), abs=abs)


class ApproxStr:
    """Pytest helper in style of `pytest.approx`, but for string checking, based on prefix, body and or suffix"""

    def __init__(
            self,
            prefix: Optional[str] = None,
            body: Optional[str] = None,
            suffix: Optional[str] = None,
    ):
        # TODO: option to do case-insensitive comparison?
        self.prefix = prefix
        self.body = body
        self.suffix = suffix

    def __eq__(self, other):
        return isinstance(other, str) and \
               (self.prefix is None or other.startswith(self.prefix)) and \
               (self.body is None or self.body in other) and \
               (self.suffix is None or other.endswith(self.suffix))

    def __repr__(self):
        return "...".join([self.prefix or ""] + ([self.body] if self.body else []) + [self.suffix or ""])


def approx_str_prefix(prefix: str) -> ApproxStr:
    return ApproxStr(prefix=prefix)


def approx_str_contains(body: str) -> ApproxStr:
    return ApproxStr(body=body)


def approx_str_suffix(suffix: str) -> ApproxStr:
    return ApproxStr(suffix=suffix)


class SameRepr:
    """
    Pytest assert helper to check if actual value has same `repr` serialization as expected.
    """

    def __init__(self, expected: Any):
        self.expected = expected

    def __eq__(self, other):
        return repr(other) == repr(self.expected)


# Function looking alias
same_repr = SameRepr


def clock_mock(
        start: Union[None, int, float, str, datetime.datetime] = None,
        step: float = 0,
        offset: Optional[float] = None,
):
    """
    Mock the `time()` calls in `Clock` with a given start date/time and increment.

    :param start: change "now" to this value (given as unix epoch timestamp, or date)
    :param step: increment time with this amount each call
    :param offset: shift time forward (or backward) with this amount of seconds
    """
    if start is None:
        if offset:
            start = Clock.time() + offset
        else:
            start = 1500000000
    elif isinstance(start, str):
        start = rfc3339.parse_date_or_datetime(start)
    if isinstance(start, datetime.date) and not isinstance(start, datetime.datetime):
        start = datetime.datetime.combine(start, datetime.time())
    if isinstance(start, datetime.datetime):
        start = start.replace(tzinfo=datetime.timezone.utc).timestamp()
    assert isinstance(start, (int, float))

    return mock.patch.object(Clock, "_time", new=itertools.count(start, step=step).__next__)




class MetadataBuilder:
    """Helper for building openEO/STAC-style metadata dictionaries"""

    def capabilities(
        self,
        *,
        api_version: str = "1.1.0",
        stac_version: str = "0.9.0",
        secondary_services: bool = False,
    ) -> dict:
        """
        Helper to build a capabilities doc.
        """
        # Basic start point
        capabilities = {
            "api_version": api_version,
            "backend_version": openeo_aggregator.about.__version__,
            "stac_version": stac_version,
            "id": "openeo-aggregator-testing",
            "title": "Test openEO Aggregator",
            "description": "Test instance of openEO Aggregator",
            "endpoints": [
                {"path": "/collections", "methods": ["GET"]},
                {"path": "/collections/{collection_id}", "methods": ["GET"]},
                {"path": "/processes", "methods": ["GET"]},
            ],
            "links": [],
        }

        # Additional features
        if secondary_services:
            capabilities["endpoints"].extend(
                [
                    {"path": "/service_types", "methods": ["GET"]},
                    {"path": "/services", "methods": ["GET", "POST"]},
                    {
                        "path": "/services/{service_id}",
                        "methods": ["GET", "PATCH", "DELETE"],
                    },
                    {"path": "/services/{service_id}/logs", "methods": ["GET"]},
                ]
            )

        return capabilities

    def collection(self, id="S2", *, license="proprietary") -> dict:
        """Build collection metadata"""
        return {
            "id": id,
            "license": license,
            "stac_version": "1.0.0",
            "description": id,
            "extent": {
                "spatial": {"bbox": [[2, 50, 5, 55]]},
                "temporal": {"interval": [["2017-01-01T00:00:00Z", None]]},
            },
            "links": [
                {
                    "rel": "license",
                    "href": "https://oeoa.test/licence",
                }
            ],
        }

    def collections(self, *args) -> dict:
        """Build `GET /collections` metadata"""
        collections = []
        for arg in args:
            if isinstance(arg, str):
                collection = self.collection(id=arg)
            elif isinstance(arg, dict):
                collection = self.collection(**arg)
            else:
                raise ValueError(arg)
            collections.append(collection)

        return {"collections": collections, "links": []}

    def process(
        self,
        id,
        *,
        parameters: Optional[List[dict]] = None,
        returns: Optional[dict] = None,
    ) -> dict:
        """Build process metadata"""
        return {
            "id": id,
            "description": id,
            "parameters": parameters or [],
            "returns": returns
            or {"schema": {"type": "object", "subtype": "raster-cube"}},
        }

    def processes(self, *args) -> dict:
        """Build `GET /processes` metadata"""
        processes = []
        for arg in args:
            if isinstance(arg, str):
                process = self.collection(id=arg)
            elif isinstance(arg, dict):
                process = self.collection(**arg)
            else:
                raise ValueError(arg)
            processes.append(process)

        return {"processes": processes, "links": []}
