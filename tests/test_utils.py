import pytest

from openeo_aggregator.utils import TtlCache, CacheMissException, MultiDictGetter


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


class TestMultiDictGetter:

    def test_basic(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert list(getter.get("a")) == [1]
        assert list(getter.get("b")) == [2, 222]
        assert list(getter.get("c")) == [333]
        assert list(getter.get("d")) == []

    def test_union(self):
        getter = MultiDictGetter([{"a": [1, 11], "b": [2, 22], "c": [33]}, {"b": [222, 2222], "c": [33, 3333]}])
        assert getter.union("a") == [1, 11]
        assert getter.union("b") == [2, 22, 222, 2222]
        assert getter.union("c") == [33, 33, 3333]
        assert getter.union("c", skip_duplicates=True) == [33, 3333]
        assert getter.union("d") == []

    def test_first(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert getter.first("a") == 1
        assert getter.first("b") == 2
        assert getter.first("c") == 333
        assert getter.first("d") is None
        assert getter.first("d", default=666) == 666

    def test_select(self):
        getter = MultiDictGetter([
            {"a": {"aa": {"aaa": 1, "aab": 2}, "ab": 3}, "b": {"ba": 4, "bb": {"bba": 5}}},
            {"a": {"aa": {"aaa": 10, "aac": 12}, "ac": 13}, "b": {"ba": 14, "bc": {"bbc": 15}}},
        ])
        assert list(getter.select("a").get("aa")) == [{"aaa": 1, "aab": 2}, {"aaa": 10, "aac": 12}]
        assert list(getter.select("a").get("ab")) == [3]
        assert list(getter.select("b").get("a")) == []
        assert list(getter.select("b").get("ba")) == [4, 14]
        assert list(getter.select("b").get("bb")) == [{"bba": 5}]
        assert list(getter.select("b").get("bc")) == [{"bbc": 15}]
        assert list(getter.select("a").select("aa").get("aaa")) == [1, 10]
        assert list(getter.select("a").select("aa").get("aab")) == [2]
        assert list(getter.select("a").select("aa").get("aac")) == [12]
        assert list(getter.select("a").select("aa").get("aad")) == []
        assert list(getter.select("b").select("ba").get("x")) == []
        assert list(getter.select("b").select("bb").get("bba")) == [5]
        assert list(getter.select("x").select("y").select("z").get("z")) == []
