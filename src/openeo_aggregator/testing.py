import dataclasses
import datetime
import itertools
import json
import pathlib
from typing import Any, Dict, List, Optional, Tuple, Union
from unittest import mock

import kazoo
import kazoo.exceptions
import openeo_driver.testing
import pytest
from openeo.util import rfc3339

import openeo_aggregator.about
import openeo_aggregator.config
from openeo_aggregator.utils import Clock


@dataclasses.dataclass
class DummyZnodeStat:
    last_modified: float = dataclasses.field(default_factory=Clock.time)


class DummyKazooClient:
    """
    Stand-in object for KazooClient for testing.
    """

    def __init__(self):
        self.state = "closed"
        self.data: Dict[str, Tuple[bytes, DummyZnodeStat]] = {}

    def start(self, timeout: float = 15):
        assert self.state == "closed"
        self.state = "open"

    def stop(self):
        assert self.state == "open"
        self.state = "closed"

    @property
    def connected(self):
        return self.state == "open"

    def _assert_open(self):
        if not self.state == "open":
            raise kazoo.exceptions.ConnectionClosedError("Connection has been closed")

    def create(self, path: str, value, makepath: bool = False):
        self._assert_open()
        if path in self.data:
            raise kazoo.exceptions.NodeExistsError()
        parent = str(pathlib.Path(path).parent)
        if parent not in self.data and parent != path:
            if makepath:
                self.create(parent, b"", makepath=makepath)
            else:
                raise kazoo.exceptions.NoNodeError
        self.data[path] = value, DummyZnodeStat()

    def exists(self, path):
        self._assert_open()
        return path in self.data

    def get(self, path):
        self._assert_open()
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        return self.data[path]

    def get_children(self, path):
        self._assert_open()
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        parent = path.split("/")
        return [p.split("/")[-1] for p in self.data if p.split("/")[:-1] == parent]

    def set(self, path, value, version=-1):
        self._assert_open()
        if path not in self.data:
            raise kazoo.exceptions.NoNodeError()
        self.data[path] = (value, DummyZnodeStat())

    def get_data_deserialized(self, drop_empty=False) -> dict:
        """Dump whole db as a dict, but deserialize values"""

        def deserialize(b: bytes):
            # simple JSON format sniffing
            if (b[:1], b[-1:]) in {(b"{", b"}"), (b"[", b"]")}:
                return json.loads(b.decode("utf8"))
            else:
                return b.decode("utf8")

        return {k: deserialize(v) for (k, (v, stats)) in self.data.items() if v or not drop_empty}


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
        return (
            isinstance(other, str)
            and (self.prefix is None or other.startswith(self.prefix))
            and (self.body is None or self.body in other)
            and (self.suffix is None or other.endswith(self.suffix))
        )

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
                {"path": "/validation", "methods": ["POST"]},
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

    def credentials_oidc(self, id="egi", issuer="https://egi.test", title="EGI", extra: Optional[List[dict]] = None):
        providers = [{"id": id, "issuer": issuer, "title": title}]
        if extra:
            providers += extra
        return {"providers": providers}

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
            "returns": returns or {"schema": {"type": "object", "subtype": "raster-cube"}},
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


def config_overrides(**kwargs):
    """
    *Only to be used in unit tests*

    `mock.patch` based mocker to override the config returned by `get_backend_config()` at run time

    Can be used as context manager

        >>> with config_overrides(id="foobar"):
        ...     ...

    in a fixture (as context manager):

        >>> @pytest.fixture
        ... def custom_setup()
        ...     with config_overrides(id="foobar"):
        ...         yield

    or as test function decorator

        >>> @config_overrides(id="foobar")
        ... def test_stuff():
    """
    return openeo_driver.testing.config_overrides(
        config_getter=openeo_aggregator.config._config_getter,
        **kwargs,
    )
