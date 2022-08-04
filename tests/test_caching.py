import logging

import pytest

from openeo_aggregator.caching import TtlCache, CacheMissException


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
