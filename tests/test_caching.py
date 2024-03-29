import collections
import itertools
import logging
from typing import Callable
from unittest import mock

import kazoo.exceptions
import pytest

import openeo_aggregator.caching
from openeo_aggregator.backend import _InternalCollectionMetadata
from openeo_aggregator.caching import (
    CacheMissException,
    ChainedMemoizer,
    DictMemoizer,
    JsonDictMemoizer,
    JsonSerDe,
    NullMemoizer,
    TtlCache,
    ZkMemoizer,
    json_serde,
    memoizer_from_config,
)
from openeo_aggregator.testing import DummyZnodeStat, clock_mock, config_overrides
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


class TestJsonSerde:
    @pytest.mark.parametrize(
        ["value", "serialized"],
        [
            ({"foo": 123}, b'{"foo":123}'),
            ({"foo": [1, 2, 3]}, b'{"foo":[1,2,3]}'),
        ],
    )
    def test_default(self, value, serialized):
        serde = JsonSerDe()
        assert serde.serialize(value) == serialized
        assert serde.deserialize(serialized) == value

    def test_custom_serialization_basic(self):
        serde = JsonSerDe()

        @serde.register_custom_codec
        class Balloon:
            def __init__(self, color: str):
                self._color = color

            def describe(self):
                return f"a {self._color} balloon"

            def __jsonserde_prepare__(self):
                return {"color": self._color}

            @classmethod
            def __jsonserde_load__(cls, data: dict):
                return cls(**data)

        data = {"alice": Balloon("red")}
        serialized = serde.serialize(data)
        assert isinstance(serialized, bytes)

        result = serde.deserialize(serialized)
        assert isinstance(result["alice"], Balloon)
        assert result["alice"].describe() == "a red balloon"

    def test_global_json_serde(self):
        icm = _InternalCollectionMetadata()
        icm.set_backends_for_collection(cid="S2", backends=["b5", "b9"])
        data = {"color": "green", "icm": icm}

        serialized = json_serde.serialize(data)
        assert isinstance(serialized, bytes)
        assert b'"color":"green"' in serialized
        assert b'"backends":["b5","b9"]' in serialized

        decoded = json_serde.deserialize(serialized)
        assert isinstance(decoded["icm"], _InternalCollectionMetadata)
        assert decoded["icm"].get_backends_for_collection("S2") == ["b5", "b9"]

    def test_additional_gzip(self):
        serde = JsonSerDe(gzip_threshold=1000)
        data = {"data": "a" * 1000}
        serialized = serde.serialize(data)
        assert isinstance(serialized, bytes)
        assert len(serialized) < 100
        assert serde.deserialize(serialized) == data


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

        assert cache.get_or_call(key="data", callback=callback) == {"ids": [1, 2, 3], "size": (4, 5, 6)}
        # This is expected: tuple is not supported in JSON and silently converted to list
        assert cache.get_or_call(key="data", callback=callback) == {"ids": [1, 2, 3], "size": [4, 5, 6]}

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
    def test_empty(self):
        cache = ChainedMemoizer(memoizers=[])
        callback = self._build_callback()
        assert cache.get_or_call(key="count", callback=callback) == 100
        assert cache.get_or_call(key="count", callback=callback) == 101
        assert cache.get_or_call(key="count", callback=callback) == 102

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


class TestZkMemoizer(_TestMemoizer):
    @pytest.fixture
    def zk_client(self) -> mock.Mock:
        """Simple ad-hoc ZooKeeper client fixture using a dictionary for storage."""
        # TODO: unify with DummyKazooClient?
        zk_client = mock.Mock()
        db = {}
        zk_client.connected = False

        def start(*args, **kwargs):
            zk_client.connected = True

        def stop():
            zk_client.connected = False

        def assert_connected():
            if not zk_client.connected:
                raise kazoo.exceptions.ConnectionClosedError("Connection has been closed")

        def get(path):
            assert_connected()
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            return db[path]

        def create(path, value, makepath=False):
            assert_connected()
            if path in db:
                raise kazoo.exceptions.NodeExistsError
            assert isinstance(value, bytes)
            db[path] = (value, DummyZnodeStat(last_modified=Clock.time()))

        def set(path, value):
            assert_connected()
            if path not in db:
                raise kazoo.exceptions.NoNodeError
            assert isinstance(value, bytes)
            db[path] = (value, DummyZnodeStat(last_modified=Clock.time()))

        zk_client.start.side_effect = start
        zk_client.stop.side_effect = stop
        zk_client.get.side_effect = get
        zk_client.create.side_effect = create
        zk_client.set.side_effect = set

        return zk_client

    def test_basic(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")
        callback = self._build_callback()
        zk_client.get.assert_not_called()
        assert zk_cache.get_or_call(key="count", callback=callback) == 100
        zk_client.get.assert_called_once_with(path="/test/count")

    @pytest.mark.parametrize(
        ["side_effects", "expected_error"],
        [
            ({"start": RuntimeError}, "failed to start connection"),
            ({"start": kazoo.exceptions.KazooException}, "failed to start connection"),
            ({"get": RuntimeError}, "unexpected get failure"),
            ({"get": kazoo.exceptions.KazooException}, "unexpected get failure"),
            ({"create": RuntimeError}, "failed to create path '/test/count'"),
            ({"create": kazoo.exceptions.KazooException}, "failed to create path '/test/count'"),
            (
                {
                    "get": (lambda *arg, **kwargs: ('{"foo":"bar"}', DummyZnodeStat(last_modified=123))),
                    "set": RuntimeError,
                },
                "failed to set path '/test/count'",
            ),
            (
                {
                    "get": (lambda *arg, **kwargs: ('{"foo":"bar"}', DummyZnodeStat(last_modified=123))),
                    "set": kazoo.exceptions.KazooException,
                },
                "failed to set path '/test/count'",
            ),
            ({"stop": RuntimeError}, "failed to stop connection"),
            ({"stop": kazoo.exceptions.KazooException}, "failed to stop connection"),
        ],
    )
    def test_broken_client(self, caplog, side_effects, expected_error):
        """Test that callback keeps working if ZooKeeper client is broken"""
        caplog.set_level(logging.ERROR)

        zk_client = mock.Mock()
        zk_client.connected = False
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
            assert "invalidated path '/test/count'" in caplog.text
        with clock_mock(2000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 101
        with clock_mock(3000):
            assert zk_cache.get_or_call(key="count", callback=callback) == 102

    @pytest.mark.parametrize(
        ["prefix", "key", "path"],
        [
            ("test", "count", "/test/count"),
            ("/test/", "/count/", "/test/count"),
            ("test/v1", "user/count/", "/test/v1/user/count"),
            ("test", ("count",), "/test/count"),
            ("test", ("user", "count"), "/test/user/count"),
            ("test", ("v1", "user", "count", "today"), "/test/v1/user/count/today"),
            ("test", ["v1", "user", "count", "today"], "/test/v1/user/count/today"),
        ],
    )
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

    @pytest.mark.parametrize(
        ["data", "expected_prefix"],
        [
            (1234, b"1234"),
            ("just a string", b'"just'),
            ({"color": "green", "sizes": [1, 2, 3]}, b'{"color":"green'),
            ([{"id": 3}, {"id": 99}], b'[{"id":'),
            ([123, None, False, True, "foo"], b"[123"),
        ],
    )
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

        zk_client.start()
        assert zk_client.get("/test/count")[0] == b"100"
        zk_client.set("/test/count", value=b"(orrup[ JS0N d@ta !*")
        assert zk_client.get("/test/count")[0] == b"(orrup[ JS0N d@ta !*"

        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_cache.get_or_call(key="count", callback=callback) == 101
        assert zk_client.get("/test/count")[0] == b"101"

        assert "corrupt data path '/test/count'" in caplog.text

    @clock_mock(0)
    def test_from_config(self, zk_client):
        with config_overrides(
            memoizer={
                "type": "zookeeper",
                "config": {
                    "zk_hosts": "zk1.test:2181,zk2.test:2181",
                    "default_ttl": 123,
                    "zk_timeout": 7.25,
                },
            }
        ), mock.patch.object(openeo_aggregator.caching, "KazooClient", return_value=zk_client) as KazooClient:
            zk_cache = memoizer_from_config(namespace="tezt")

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

    def test_connect_context_nesting(self, zk_client):
        zk_cache = ZkMemoizer(client=zk_client, path_prefix="test")

        counter = self._build_callback(start=100)

        def fun1():
            return f"fun1:{counter()}"

        def fun1_cached():
            return zk_cache.get_or_call("fun1", fun1)

        def fun2():
            return fun1_cached() + f"+fun2:{counter()}"

        def fun2_cached():
            return zk_cache.get_or_call("fun2", fun2)

        zk_client.get.assert_not_called()

        assert fun2_cached() == "fun1:100+fun2:101"
        zk_client.start.assert_called_once()
        zk_client.stop.assert_called_once()
        assert fun2_cached() == "fun1:100+fun2:101"


class TestMemoizerFromConfig:
    def test_null_memoizer(self):
        with config_overrides(memoizer={"type": "null"}):
            memoizer = memoizer_from_config(namespace="test")
        assert isinstance(memoizer, NullMemoizer)

    def test_dict_memoizer(self):
        with config_overrides(memoizer={"type": "dict", "config": {"default_ttl": 99}}):
            memoizer = memoizer_from_config(namespace="test")
        assert isinstance(memoizer, DictMemoizer)
        assert memoizer._default_ttl == 99

    def test_jsondict_memoizer(self):
        with config_overrides(memoizer={"type": "jsondict", "config": {"default_ttl": 99}}):
            memoizer = memoizer_from_config(namespace="test")
        assert isinstance(memoizer, JsonDictMemoizer)
        assert memoizer._default_ttl == 99

    def test_zookeeper_memoizer(self):
        with config_overrides(
            memoizer={"type": "zookeeper", "config": {"zk_hosts": "zk.test:2181", "default_ttl": 99, "zk_timeout": 88}},
            zookeeper_prefix="/oea/test",
        ):
            memoizer = memoizer_from_config(namespace="test-ns")
        assert isinstance(memoizer, ZkMemoizer)
        assert memoizer._default_ttl == 99
        assert memoizer._prefix == "/oea/test/cache/test-ns"
        assert memoizer._zk_timeout == 88

    def test_chained_memoizer(self):
        with config_overrides(
            memoizer={
                "type": "chained",
                "config": {
                    "parts": [
                        {"type": "jsondict", "config": {"default_ttl": 99}},
                        {"type": "dict", "config": {"default_ttl": 333}},
                    ]
                },
            }
        ):
            memoizer = memoizer_from_config(namespace="test-ns")
        assert isinstance(memoizer, ChainedMemoizer)
        assert len(memoizer._memoizers) == 2
        assert isinstance(memoizer._memoizers[0], JsonDictMemoizer)
        assert memoizer._memoizers[0]._default_ttl == 99
        assert isinstance(memoizer._memoizers[1], DictMemoizer)
        assert memoizer._memoizers[1]._default_ttl == 333
