import collections
import itertools
import logging
from typing import Callable
from unittest import mock

import kazoo.exceptions
import pytest

import openeo_aggregator.caching
from openeo_aggregator.caching import TtlCache, CacheMissException, ZkMemoizer, memoizer_from_config, NullMemoizer, \
    DictMemoizer, ChainedMemoizer, JsonDictMemoizer
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


class _TestMemoizer:
    def _build_callback(self, start=100, step=1) -> Callable[[], int]:
        """Build callback that just returns increasing numbers on each invocation"""
        return itertools.count(start=start, step=step).__next__

    def _build_failing_callback(self, start=100, step=1, fail_mod=2):
        stream = itertools.count(start=start, step=1)

        def callback():
            x = next(stream)
            if x % fail_mod == 0:
                raise ValueError(x)
            return x

        return callback


class TestNullMemoizer(_TestMemoizer):

    def test_basic(self):
        cache = NullMemoizer()
        callback = self._build_callback()
        assert cache.get_or_call(key="count", callback=callback) == 100
        assert cache.get_or_call(key="count", callback=callback) == 101


class TestDictMemoizer(_TestMemoizer):
    def test_basic(self):
        cache = DictMemoizer()
        callback = self._build_callback()
        assert cache.get_or_call(key="count", callback=callback) == 100
        assert cache.get_or_call(key="count", callback=callback) == 100

    def test_failing_callback(self):
        cache = DictMemoizer(default_ttl=66)
        callback = self._build_failing_callback(start=999, fail_mod=2)
        with clock_mock(100):
            assert cache.get_or_call(key="count", callback=callback) == 999
        with clock_mock(200):
            # Second call fails
            with pytest.raises(ValueError, match="1000"):
                cache.get_or_call(key="count", callback=callback)
            # Third call works again
            assert cache.get_or_call(key="count", callback=callback) == 1001

    def test_expiry(self):
        cache = DictMemoizer(default_ttl=100)
        callback = self._build_callback()
        with clock_mock(1000):
            assert cache.get_or_call(key="count", callback=callback) == 100
            assert cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1099):
            assert cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1100):
            assert cache.get_or_call(key="count", callback=callback) == 101
            assert cache.get_or_call(key="count", callback=callback) == 101
        with clock_mock(1150):
            assert cache.get_or_call(key="count", callback=callback) == 101
            assert cache.get_or_call(key="count", callback=callback, ttl=10) == 102

    def test_invalidate(self):
        cache = DictMemoizer(default_ttl=100)
        callback = self._build_callback()
        assert cache.get_or_call(key="count", callback=callback) == 100
        assert cache.get_or_call(key="count", callback=callback) == 100
        cache.invalidate()
        assert cache.get_or_call(key="count", callback=callback) == 101

    def test_decorator(self):
        cache = DictMemoizer(default_ttl=100)

        _state = [1, 22, 333]

        @cache.wrap(key="count", ttl=60)
        def count():
            return _state.pop()

        with clock_mock(1000):
            assert count() == 333
            assert count() == 333
        with clock_mock(1061):
            assert count() == 22
            assert count() == 22
        with clock_mock(2000):
            assert count() == 1


class TestJsonDictMemoizer(TestDictMemoizer):

    def test_json_coercion(self):
        cache = JsonDictMemoizer()
        callback = lambda: {"ids": [1, 2, 3], "size": (4, 5, 6)}

        assert cache.get_or_call(key="data", callback=callback) == {'ids': [1, 2, 3], 'size': (4, 5, 6)}
        # This is expected: tuple is not supported in JSON and silently converted to list
        assert cache.get_or_call(key="data", callback=callback) == {'ids': [1, 2, 3], 'size': [4, 5, 6]}

    def test_json_encode_error(self, caplog):
        caplog.set_level(logging.ERROR)
        cache = JsonDictMemoizer()

        class Foo:
            _state = [1, 22, 333]

            def __init__(self):
                self.x = self._state.pop()

        # No caching, but callback keeps working
        res = cache.get_or_call(key="data", callback=Foo)
        assert isinstance(res, Foo) and res.x == 333
        assert "failed to memoize" in caplog.text
        assert "Foo is not JSON serializable" in caplog.text

        res = cache.get_or_call(key="data", callback=Foo)
        assert isinstance(res, Foo) and res.x == 22


class TestChainedMemoizer(_TestMemoizer):

    def test_simple(self):
        dm1 = DictMemoizer(default_ttl=10)
        dm2 = DictMemoizer(default_ttl=100)
        cache = ChainedMemoizer(memoizers=[dm1, dm2])
        callback = self._build_callback()

        with clock_mock(1000):
            assert cache.get_or_call(key="count", callback=callback) == 100
            assert cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1020):
            assert cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1200):
            assert cache.get_or_call(key="count", callback=callback) == 101

    def test_levels(self):
        callback = self._build_callback(start=11, step=11)
        dm1 = DictMemoizer(default_ttl=10)
        dm2 = DictMemoizer(default_ttl=200)
        dm3 = DictMemoizer(default_ttl=3000)

        # Prime caches differently
        with clock_mock(10000):
            assert dm1.get_or_call(key="count", callback=callback) == 11
            assert dm2.get_or_call(key="count", callback=callback) == 22
            assert dm3.get_or_call(key="count", callback=callback) == 33

        chained = ChainedMemoizer(memoizers=[dm1, dm2, dm3])

        # Cache hit in first level
        with clock_mock(10005):
            assert chained.get_or_call(key="count", callback=callback) == 11
            assert dm1.get_or_call(key="count", callback=callback) == 11
            assert dm2.get_or_call(key="count", callback=callback) == 22
            assert dm3.get_or_call(key="count", callback=callback) == 33

        # First level expired
        with clock_mock(10015):
            assert chained.get_or_call(key="count", callback=callback) == 22
            assert dm1.get_or_call(key="count", callback=callback) == 22
            assert dm2.get_or_call(key="count", callback=callback) == 22
            assert dm3.get_or_call(key="count", callback=callback) == 33
        # Second level expired
        with clock_mock(10250):
            assert chained.get_or_call(key="count", callback=callback) == 33
            assert dm1.get_or_call(key="count", callback=callback) == 33
            assert dm2.get_or_call(key="count", callback=callback) == 33
            assert dm3.get_or_call(key="count", callback=callback) == 33
        # All level expired
        with clock_mock(13500):
            assert chained.get_or_call(key="count", callback=callback) == 44
            assert dm1.get_or_call(key="count", callback=callback) == 44
            assert dm2.get_or_call(key="count", callback=callback) == 44
            assert dm3.get_or_call(key="count", callback=callback) == 44

    def test_invalidate(self):
        dm1 = DictMemoizer(default_ttl=10)
        dm2 = DictMemoizer(default_ttl=100)
        cache = ChainedMemoizer(memoizers=[dm1, dm2])
        callback = self._build_callback()

        assert cache.get_or_call(key="count", callback=callback) == 100
        assert cache.get_or_call(key="count", callback=callback) == 100
        cache.invalidate()
        assert cache.get_or_call(key="count", callback=callback) == 101
        assert dm1.get_or_call(key="count", callback=callback) == 101
        assert dm2.get_or_call(key="count", callback=callback) == 101

    def test_failing_callback(self, caplog):
        dm1 = DictMemoizer(default_ttl=10)
        dm2 = DictMemoizer(default_ttl=100)
        cache = ChainedMemoizer(memoizers=[dm1, dm2])
        callback = self._build_failing_callback(start=999, fail_mod=2)

        with clock_mock(100):
            assert cache.get_or_call(key="count", callback=callback) == 999
            assert dm1.get_or_call(key="count", callback=callback) == 999
            assert dm2.get_or_call(key="count", callback=callback) == 999
        with clock_mock(150):
            assert cache.get_or_call(key="count", callback=callback) == 999
        with clock_mock(250):
            # Second call fails
            with pytest.raises(ValueError, match="1000"):
                _ = cache.get_or_call(key="count", callback=callback)
            # Third call works again
            assert cache.get_or_call(key="count", callback=callback) == 1001
            assert dm1.get_or_call(key="count", callback=callback) == 1001
            assert dm2.get_or_call(key="count", callback=callback) == 1001
        with clock_mock(400):
            # evaluate wrapped caches first now
            with pytest.raises(ValueError, match="1002"):
                _ = dm1.get_or_call(key="count", callback=callback)
            # call on wrapped caches first now
            assert dm2.get_or_call(key="count", callback=callback) == 1003
            with pytest.raises(ValueError, match="1004"):
                _ = dm1.get_or_call(key="count", callback=callback)
            assert cache.get_or_call(key="count", callback=callback) == 1003
            assert dm1.get_or_call(key="count", callback=callback) == 1003
            assert dm2.get_or_call(key="count", callback=callback) == 1003


DummyZnodeStat = collections.namedtuple("DummyZnodeStat", ["last_modified"])


class TestZkMemoizer(_TestMemoizer):

    @pytest.fixture
    def zk_client(self) -> mock.Mock:
        """Simple ad-hoc ZooKeeper client fixture using a dictionary for storage."""
        zk_client = mock.Mock()
        db = {}

        def get(path):
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            return db[path]

        def create(path, value, makepath=False):
            if path in db:
                raise kazoo.exceptions.NodeExistsError
            assert isinstance(value, bytes)
            db[path] = (value, DummyZnodeStat(last_modified=Clock.time()))

        def set(path, value):
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            assert isinstance(value, bytes)
            db[path] = (value, DummyZnodeStat(last_modified=Clock.time()))

        zk_client.get.side_effect = get
        zk_client.create.side_effect = create
        zk_client.set.side_effect = set
        return zk_client

    def test_basic(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        zk_client.get.assert_called_once_with(path='/test/count')

    @pytest.mark.parametrize(["side_effects", "expected_error"], [
        ({"start": RuntimeError}, "failed to start connection"),
        ({"start": kazoo.exceptions.KazooException}, "failed to start connection"),
        ({"get": RuntimeError}, "unexpected get failure"),
        ({"get": kazoo.exceptions.KazooException}, "unexpected get failure"),
        ({"create": RuntimeError}, "failed to create '/test/count'"),
        ({"create": kazoo.exceptions.KazooException}, "failed to create '/test/count'"),
        ({
             "get": (lambda *arg, **kwargs: ('{"foo":"bar"}', DummyZnodeStat(last_modified=123))),
             "set": RuntimeError,
         }, "failed to set '/test/count'"),
        ({
             "get": (lambda *arg, **kwargs: ('{"foo":"bar"}', DummyZnodeStat(last_modified=123))),
             "set": kazoo.exceptions.KazooException,
         }, "failed to set '/test/count'"),
        ({"stop": RuntimeError}, "failed to stop connection"),
        ({"stop": kazoo.exceptions.KazooException}, "failed to stop connection"),
    ])
    def test_broken_client(self, caplog, side_effects, expected_error):
        """Test that callback keeps working if ZooKeeper client is broken"""
        caplog.set_level(logging.ERROR)

        zk_client = mock.Mock()
        zk_client.start = mock.Mock(side_effect=side_effects.get("start"))
        zk_client.get = mock.Mock(side_effect=side_effects.get("get", kazoo.exceptions.NoNodeError))
        zk_client.create = mock.Mock(side_effect=side_effects.get("create"))
        zk_client.set = mock.Mock(side_effect=side_effects.get("set"))
        zk_client.stop = mock.Mock(side_effect=side_effects.get("stop"))

        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")

        callback = self._build_callback()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100

        if expected_error:
            assert expected_error in caplog.text

        # No caching, but still getting results from callback
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_cache.get_or_call(key="count", callback=callback) == 102

    def test_basic_caching(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100

    def test_failing_callback(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=66)
        callback = self._build_failing_callback(start=999, fail_mod=2)
        with clock_mock(100):
            assert zk_cache.get_or_call(key="count", callback=callback) == 999
        with clock_mock(200):
            # Second call fails
            with pytest.raises(ValueError, match="1000"):
                zk_cache.get_or_call(key="count", callback=callback)
            # Third call works again
            assert zk_cache.get_or_call(key="count", callback=callback) == 1001

    @clock_mock(0)
    def test_caching_expiry(self, zk_client):
        callback = self._build_callback()
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=1000)
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

    @clock_mock(0)
    def test_invalidate(self, zk_client, caplog):
        caplog.set_level(logging.DEBUG)
        callback = self._build_callback()
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=1000)
        with clock_mock(1000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1500):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
            zk_cache.invalidate()
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
            assert "invalidated '/test/count'" in caplog.text
        with clock_mock(2000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
        with clock_mock(3000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 102

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
        zk_cache = ZkMemoizer(client=zk_client, path_prefix=prefix)
        callback = self._build_callback()
        assert zk_cache.get_or_call(key=key, callback=callback) == 100
        zk_client.get.assert_called_once_with(path=path)
        # TODO test for having non-strings parts in key?

    def test_caching_create_vs_set(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=100)
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

    def test_create_on_existing_node(self, zk_client, caplog):
        # Force "NodeExistsErr from `create`
        zk_client.create.side_effect = kazoo.exceptions.NodeExistsError
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=100)
        callback = self._build_callback()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert "failed to create node '/test/count': already exists." in caplog.text

    @pytest.mark.parametrize(["data", "expected_prefix"], [
        (1234, b"1234"),
        ("just a string", b'"just'),
        ({"color": "green", "sizes": [1, 2, 3]}, b'{"color":"green'),
        ([{"id": 3}, {"id": 99}], b'[{"id":'),
        ([123, None, False, True, "foo"], b'[123'),
    ])
    def test_serializing(self, zk_client, data, expected_prefix):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")

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

    def test_corrupted_cache(self, zk_client, caplog):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test", default_ttl=100)
        callback = self._build_callback()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        assert zk_cache.get_or_call(key="count", callback=callback) == 100

        assert zk_client.get("/test/count")[0] == b"100"
        zk_client.set("/test/count", value=b"(orrup[ JS0N d@ta !*")
        assert zk_client.get("/test/count")[0] == b"(orrup[ JS0N d@ta !*"

        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_client.get("/test/count")[0] == b"101"

        assert "corrupt data on '/test/count'" in caplog.text

    @clock_mock(0)
    def test_from_config(self, config, zk_client):
        config.memoizer = {
            "type": "zookeeper",
            "config": {
                "zk_hosts": "zk1.test:2181,zk2.test:2181",
                "default_ttl": 123,
                "zk_timeout": 7.25,
            }
        }
        with mock.patch.object(openeo_aggregator.caching, "KazooClient", return_value=zk_client) as KazooClient:
            zk_cache = memoizer_from_config(config, namespace="tezt")

        KazooClient.assert_called_with(hosts="zk1.test:2181,zk2.test:2181")

        callback = self._build_callback()
        with clock_mock(1000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1122):
            assert zk_cache.get_or_call(key="count", callback=callback) == 100
        with clock_mock(1123):
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
            assert zk_cache.get_or_call(key="count", callback=callback) == 101

        zk_client.start.assert_called_with(timeout=7.25)

        paths = set(c[2]["path"] for c in zk_client.mock_calls if c[0] in {"get", "set"})
        assert paths == {"/o-a/cache/tezt/count"}
