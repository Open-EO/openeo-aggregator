import collections
import itertools
import logging
from typing import Callable
from unittest import mock

import kazoo.exceptions
import pytest

from openeo_aggregator.caching import TtlCache, CacheMissException, ZkMemoizer
from openeo_aggregator.testing import clock_mock
from openeo_aggregator.utils import Clock


class FakeClock:
    def __init__(self, now: float = 0):
        self.now = now

    def set(self, now: float):
        self.now = now

    def __call__(self):
        return self.now


class TestTtlCache:

    def test_basic(self):
        cache = TtlCache()
        cache.set("foo", "bar")
        assert "foo" in cache
        assert "meh" not in cache
        assert cache.get("foo") == "bar"
        assert cache["foo"] == "bar"
        assert cache.get("meh") is None
        with pytest.raises(CacheMissException):
            _ = cache["meh"]

    def test_get_default(self):
        cache = TtlCache()
        assert cache.get("foo") is None
        assert cache.get("foo", 123) == 123

    def test_default_ttl(self):
        clock = FakeClock()
        cache = TtlCache(default_ttl=10, clock=clock)
        clock.set(100)
        cache.set("foo", "bar")
        clock.set(105)
        assert cache.get("foo") == "bar"
        clock.set(110)
        assert cache.get("foo") == "bar"
        clock.set(115)
        assert cache.get("foo") is None
        with pytest.raises(CacheMissException):
            _ = cache["foo"]

    def test_item_ttl(self):
        clock = FakeClock()
        cache = TtlCache(default_ttl=10, clock=clock)
        clock.set(100)
        cache.set("foo", "bar", ttl=20)
        clock.set(105)
        assert cache.get("foo") == "bar"
        clock.set(115)
        assert cache.get("foo") == "bar"
        clock.set(125)
        assert cache.get("foo") is None
        with pytest.raises(CacheMissException):
            _ = cache["foo"]

    def test_get_or_call(self):
        clock = FakeClock()
        cache = TtlCache(default_ttl=10, clock=clock)
        clock.set(100)
        callback = iter([1, 22, 333, 4444]).__next__
        assert cache.get_or_call("foo", callback) == 1
        assert cache.get_or_call("foo", callback) == 1
        clock.set(105)
        assert cache.get_or_call("foo", callback) == 1
        clock.set(114)
        assert cache.get_or_call("foo", callback) == 22
        clock.set(118)
        assert cache.get_or_call("foo", callback) == 22
        clock.set(124)
        assert cache.get_or_call("foo", callback) == 22
        clock.set(126)
        assert cache.get_or_call("foo", callback) == 333
        clock.set(136)
        assert cache.get_or_call("foo", callback) == 333
        clock.set(137)
        assert cache.get_or_call("foo", callback) == 4444
        clock.set(200)
        with pytest.raises(StopIteration):
            cache.get_or_call("foo", callback)

    def test_get_or_call_log_on_miss(self, caplog):
        caplog.set_level(logging.DEBUG)
        cache = TtlCache(default_ttl=10, name="Kasj")
        callback = [1, 22, 333].pop
        assert cache.get_or_call("foo", callback, log_on_miss=True) == 333
        assert "Cache miss 'Kasj' key 'foo'" in caplog.text
        assert "elapsed 0:00" in caplog.text
        assert "calling 'list.pop'" in caplog.text
        caplog.clear()
        assert cache.get_or_call("foo", callback, log_on_miss=True) == 333
        assert caplog.text == ""


class TestZkMemoizer:

    def _build_callback(self, start=100) -> Callable[[], int]:
        """Build callback that just returns increasing numbers on each invocation"""
        return itertools.count(start=start).__next__

    @pytest.fixture
    def zk_client(self) -> mock.Mock:
        """Simple ad-hoc ZooKeeper client fixture using a dictionary for storage."""
        zk_client = mock.Mock()
        db = {}

        ZnodeStat = collections.namedtuple("ZnodeStat", ["last_modified"])

        def get(path):
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            return db[path]

        def create(path, value, makepath=False):
            if path in db:
                raise kazoo.exceptions.NodeExistsError
            assert isinstance(value, bytes)
            db[path] = (value, ZnodeStat(last_modified=Clock.time()))

        def set(path, value):
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            assert isinstance(value, bytes)
            db[path] = (value, ZnodeStat(last_modified=Clock.time()))

        zk_client.get.side_effect = get
        zk_client.create.side_effect = create
        zk_client.set.side_effect = set
        return zk_client

    def test_basic(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test")
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        zk_client.get.assert_called_once_with(path='/test/count')

    def test_broken_client(self):
        """Test that callback keeps working if ZooKeeper client is broken"""
        # Create useless client
        zk_client = object()
        zk_cache = ZkMemoizer(client=zk_client, prefix="test")

        callback = self._build_callback()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_cache.get_or_call(key="count", callback=callback) == 102

    def test_basic_caching(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test")
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100

    def test_caching_expiry(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test", default_ttl=1000)
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        with clock_mock(1000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1999):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(2000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
        with clock_mock(3333):
            assert zk_cache.get_or_call(key="count", callback=callback) == 102
        with clock_mock(3500):
            assert zk_cache.get_or_call(key="count", callback=callback) == 102
            assert zk_cache.get_or_call(key="count", callback=callback, ttl=60) == 103

    @pytest.mark.parametrize(["prefix", "key", "path"], [
        ("test", "count", "/test/count"),
        ("/test/", "/count/", "/test/count"),
        ("test/v1", "user/count/", "/test/v1/user/count"),
        ("test", ("count",), "/test/count"),
        ("test", ("user", "count"), "/test/user/count"),
        ("test", ("v1", "user", "count", "today"), "/test/v1/user/count/today"),
        ("test", ["v1", "user", "count", "today"], "/test/v1/user/count/today"),
    ])
    def test_key_to_path(self, zk_client, prefix, key, path):
        zk_cache = ZkMemoizer(client=zk_client, prefix=prefix)
        callback = self._build_callback()
        assert zk_cache.get_or_call(key=key, callback=callback) == 100
        zk_client.get.assert_called_once_with(path=path)
        # TODO test for having non-strings parts in key?

    def test_caching_create_vs_set(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test", default_ttl=100)
        callback = self._build_callback()
        assert (0, 0) == (zk_client.create.call_count, zk_client.set.call_count)
        with clock_mock(1000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert (1, 0) == (zk_client.create.call_count, zk_client.set.call_count)
        with clock_mock(2000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert (1, 1) == (zk_client.create.call_count, zk_client.set.call_count)
        with clock_mock(3000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 102
        assert (1, 2) == (zk_client.create.call_count, zk_client.set.call_count)

    @pytest.mark.parametrize(["data", "expected_prefix"], [
        (1234, b"1234"),
        ("just a string", b'"just'),
        ({"color": "green", "sizes": [1, 2, 3]}, b'{"color":"green'),
        ([{"id": 3}, {"id": 99}], b'[{"id":'),
        ([123, None, False, True, "foo"], b'[123'),
    ])
    def test_serializing(self, zk_client, data, expected_prefix):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test")

        callback = mock.Mock(return_value=data)

        assert callback.call_count == 0
        assert zk_cache.get_or_call(key="count", callback=callback) == data
        assert callback.call_count == 1
        assert zk_cache.get_or_call(key="count", callback=callback) == data
        assert callback.call_count == 1

        zk_client.create.assert_called_once()
        zk_value = zk_client.create.call_args[1]["value"]
        assert isinstance(zk_value, bytes)
        assert zk_value.startswith(expected_prefix)

    def test_corrupted_cache(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, prefix="test", default_ttl=100)
        callback = self._build_callback()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100

        assert zk_client.get("/test/count")[0] == b"100"
        zk_client.set("/test/count", value=b"(orrup[ JS0N d@ta !*")
        assert zk_client.get("/test/count")[0] == b"(orrup[ JS0N d@ta !*"
        
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_client.get("/test/count")[0] == b"101"
