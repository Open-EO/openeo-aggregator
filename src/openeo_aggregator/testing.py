
import datetime
import itertools
import json
import pathlib
import pytest
import time
from typing import Union
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


class _StrStartsWith:
    def __init__(self, prefix: str):
        self.prefix = prefix

    def __eq__(self, other):
        return isinstance(other, str) and other.startswith(self.prefix)

    def __repr__(self):
        return self.prefix


def str_starts_with(prefix) -> _StrStartsWith:
    """pytest helper to check if a string starts with a prefix"""
    # TODO: move to openeo_driver
    return _StrStartsWith(prefix=prefix)


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
