import logging

import pytest
import shapely.geometry

from openeo_aggregator.utils import TtlCache, CacheMissException, MultiDictGetter, subdict, dict_merge, EventHandler, \
    BoundingBox, strip_join, timestamp_to_rfc3339, normalize_issuer_url


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


class TestMultiDictGetter:

    def test_basic(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert list(getter.get("a")) == [1]
        assert list(getter.get("b")) == [2, 222]
        assert list(getter.get("c")) == [333]
        assert list(getter.get("d")) == []

    def test_concat(self):
        getter = MultiDictGetter([
            {"a": [1, 11], "b": [2, 22], "c": [33]},
            {"b": [222, 2222], "c": (33, 3333)}
        ])
        assert getter.concat("a") == [1, 11]
        assert getter.concat("b") == [2, 22, 222, 2222]
        assert getter.concat("c") == [33, 33, 3333]
        assert getter.concat("c", skip_duplicates=True) == [33, 3333]
        assert getter.concat("d") == []

    @pytest.mark.parametrize(["data", "expected", "expect_warning"], [
        ([4, 5], [1, 2, 3, 4, 5, 100], False),
        ((4, 5), [1, 2, 3, 4, 5, 100], False),
        (45, [1, 2, 3, 100], True),
        ("45", [1, 2, 3, 100], True),
        ({4: "foo", 5: "bar"}, [1, 2, 3, 100], True),
        ({"foo": 4, "bar": 5}, [1, 2, 3, 100], True),
        (range(4, 6), [1, 2, 3, 100], True),
        ((x for x in [4, 5]), [1, 2, 3, 100], True),
    ])
    def test_concat_type_handling(self, data, expected, expect_warning, caplog):
        getter = MultiDictGetter([
            {"a": [1, 2, 3], },
            {"a": data},
            {"a": [100]},
        ])
        assert getter.concat("a") == expected

        if expect_warning:
            assert "Skipping unexpected type in MultiDictGetter.concat" in caplog.text
        else:
            assert not caplog.text

    def test_union(self):
        getter = MultiDictGetter([
            {"a": [1, 11], "b": [2, 22], "c": [33]},
            {"b": [222, 2222], "c": (33, 3333)}
        ])
        assert getter.union("a") == {1, 11}
        assert getter.union("b") == {2, 22, 222, 2222}
        assert getter.union("c") == {33, 3333}
        assert getter.union("d") == set()

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


def test_subdict():
    d = {"foo": "bar", "meh": 3}
    assert subdict(d) == {}
    assert subdict(d, "foo") == {"foo": "bar"}
    assert subdict(d, "foo", "meh") == {"foo": "bar", "meh": 3}
    assert subdict(d, "foo", "bar") == {"foo": "bar", "bar": None}
    assert subdict(d, keys=["foo"]) == {"foo": "bar"}
    assert subdict(d, keys=["foo", "meh"]) == {"foo": "bar", "meh": 3}
    assert subdict(d, keys=["foo", "bar"]) == {"foo": "bar", "bar": None}
    assert subdict(d, keys=("foo", "bar")) == {"foo": "bar", "bar": None}
    assert subdict(d, keys={"foo": 0, "bar": 0}.keys()) == {"foo": "bar", "bar": None}
    assert subdict(d, "foo", keys=["meh", "bar"]) == {"foo": "bar", "meh": 3, "bar": None}


def test_dict_merge():
    assert dict_merge() == {}
    assert dict_merge({1: 2}) == {1: 2}
    assert dict_merge({1: 2}, {3: 4}) == {1: 2, 3: 4}
    assert dict_merge({1: 2}, {1: 11}) == {1: 11}
    assert dict_merge({"foo": 1, "meh": 11}, {"foo": 2, "bar": 22}, {"foo": 3}) == {"foo": 3, "bar": 22, "meh": 11}
    assert dict_merge({"foo": 1}, {"foo": 2}) == {"foo": 2}
    assert dict_merge({"foo": 1}, {"foo": 2}, foo=3) == {"foo": 3}
    assert dict_merge({"foo": 1}, {"foo": 2}, foo=3, bar=4) == {"foo": 3, "bar": 4}
    assert dict_merge({"foo": 1, "meh": 11}, {"foo": 2, "bar": 22}, foo=3, meh=55) == {"foo": 3, "bar": 22, "meh": 55}


class TestEventHandler:

    def test_empty(self):
        handler = EventHandler("event")
        handler.trigger()

    def test_simple(self):
        data = []
        handler = EventHandler("event")
        handler.add(lambda: data.append("foo"))
        assert data == []
        handler.trigger()
        assert data == ["foo"]

    def test_failure(self):
        data = []
        handler = EventHandler("event")
        handler.add(lambda: data.append("foo"))
        handler.add(lambda: data.append(4 / 0))
        handler.add(lambda: data.append("bar"))
        assert data == []
        with pytest.raises(ZeroDivisionError):
            handler.trigger()
        assert data == ["foo"]
        handler.trigger(skip_failures=True)
        assert data == ["foo", "foo", "bar"]


class TestBoundingBox:

    def test_basic(self):
        bbox = BoundingBox(1, 2, 3, 4)
        assert bbox.west == 1
        assert bbox.south == 2
        assert bbox.east == 3
        assert bbox.north == 4
        assert bbox.crs == "EPSG:4326"

    def test_from_dict(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "epsg:32633"})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "epsg:32633"

    def test_from_dict_defaults(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "EPSG:4326"

    def test_from_dict_underspecified(self):
        with pytest.raises(KeyError):
            _ = BoundingBox.from_dict({"west": 1, "south": 2, "color": "red"})

    def test_from_dict_overspecified(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326", "color": "red"})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "EPSG:4326"

    def test_as_dict(self):
        bbox = BoundingBox(1, 2, 3, 4)
        assert bbox.as_dict() == {"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"}

    def test_as_polygon(self):
        bbox = BoundingBox(1, 2, 3, 4)
        polygon = bbox.as_polygon()
        assert isinstance(polygon, shapely.geometry.Polygon)
        assert set(polygon.exterior.coords) == {(1, 2), (3, 2), (3, 4), (1, 4)}

    def test_contains(self):
        bbox = BoundingBox(1, 2, 3, 4)
        assert bbox.contains(1, 2)
        assert bbox.contains(2, 3)
        assert bbox.contains(3, 4)
        assert bbox.contains(1.4, 2.9)
        assert not bbox.contains(-1, 3)
        assert not bbox.contains(10, 3)
        assert not bbox.contains(2, 1)
        assert not bbox.contains(2, 10)


def test_strip_join():
    assert strip_join("/") == ""
    assert strip_join("/", "a") == "a"
    assert strip_join("/", "/a") == "/a"
    assert strip_join("/", "a/") == "a/"
    assert strip_join("/", "/a/") == "/a/"
    assert strip_join("/", "a", "b") == "a/b"
    assert strip_join("/", "/a/", "/b/") == "/a/b/"
    assert strip_join("/", "a", "b", "c") == "a/b/c"
    assert strip_join("/", "/a/", "/b", "/c") == "/a/b/c"
    assert strip_join("/", "/a/", "/b/", "/c/") == "/a/b/c/"


def test_timestamp_to_rfc3339():
    assert timestamp_to_rfc3339(0) == "1970-01-01T00:00:00Z"
    assert timestamp_to_rfc3339(1644012109) == "2022-02-04T22:01:49Z"


def test_normalize_issuer_url():
    assert normalize_issuer_url("https://example.com/oidc/") == "https://example.com/oidc"
    assert normalize_issuer_url("https://example.com/OidC/") == "https://example.com/oidc"
