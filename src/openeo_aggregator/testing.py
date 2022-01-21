import datetime
import itertools
import json
import pathlib
import pytest
import time
from typing import Union, Optional
from unittest import mock

import kazoo
import kazoo.exceptions

from openeo.util import rfc3339
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
        assert path not in self.data
        parent = str(pathlib.Path(path).parent)
        if parent not in self.data and parent != path:
            if makepath:
                self.create(parent, b"", makepath=makepath)
            else:
                raise kazoo.exceptions.NoNodeError
        self.data[path] = value

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
    return pytest.approx(time.time(), abs=abs)


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


def clock_mock(start: Union[int, float, str, datetime.datetime] = 1500000000, step: float = 0):
    """Mock the `time()` calls in `Clock` with a given start date/time and increment."""
    if isinstance(start, str):
        start = rfc3339.parse_date_or_datetime(start)
    if isinstance(start, datetime.date) and not isinstance(start, datetime.datetime):
        start = datetime.datetime.combine(start, datetime.time())
    if isinstance(start, datetime.datetime):
        start = start.replace(tzinfo=datetime.timezone.utc).timestamp()
    assert isinstance(start, (int, float))

    return mock.patch.object(Clock, "_time", new=itertools.count(start, step=step).__next__)
