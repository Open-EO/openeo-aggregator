import re

import pytest
import shapely.geometry

from openeo_aggregator.utils import (
    AttrStatsProxy,
    BoundingBox,
    EventHandler,
    MultiDictGetter,
    common_prefix,
    dict_merge,
    drop_dict_keys,
    is_whitelisted,
    normalize_issuer_url,
    string_or_regex_match,
    strip_join,
    subdict,
    timestamp_to_rfc3339,
)


class TestMultiDictGetter:
    def test_basic(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert list(getter.get("a")) == [1]
        assert list(getter.get("b")) == [2, 222]
        assert list(getter.get("c")) == [333]
        assert list(getter.get("d")) == []

    def test_keys(self):
        assert MultiDictGetter([]).keys() == set()
        assert MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}]).keys() == {"a", "b", "c"}
        assert MultiDictGetter([{"a": 1}, {"bb": 2}, {"ccc": 222}]).keys() == {"a", "bb", "ccc"}

    def test_has_key(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert getter.has_key("a")
        assert getter.has_key("b")
        assert getter.has_key("c")
        assert not getter.has_key("d")

    def test_available_keys(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert getter.available_keys(["a", "c", "d"]) == ["a", "c"]

    def test_concat(self):
        getter = MultiDictGetter([{"a": [1, 11], "b": [2, 22], "c": [33]}, {"b": [222, 2222], "c": (33, 3333)}])
        assert getter.concat("a") == [1, 11]
        assert getter.concat("b") == [2, 22, 222, 2222]
        assert getter.concat("c") == [33, 33, 3333]
        assert getter.concat("c", skip_duplicates=True) == [33, 3333]
        assert getter.concat("d") == []

    @pytest.mark.parametrize(
        ["data", "expected", "expect_warning"],
        [
            ([4, 5], [1, 2, 3, 4, 5, 100], False),
            ((4, 5), [1, 2, 3, 4, 5, 100], False),
            (45, [1, 2, 3, 100], True),
            ("45", [1, 2, 3, 100], True),
            ({4: "foo", 5: "bar"}, [1, 2, 3, 100], True),
            ({"foo": 4, "bar": 5}, [1, 2, 3, 100], True),
            (range(4, 6), [1, 2, 3, 100], True),
            ((x for x in [4, 5]), [1, 2, 3, 100], True),
        ],
    )
    def test_concat_type_handling(self, data, expected, expect_warning, caplog):
        getter = MultiDictGetter(
            [
                {
                    "a": [1, 2, 3],
                },
                {"a": data},
                {"a": [100]},
            ]
        )
        assert getter.concat("a") == expected

        if expect_warning:
            assert "skipping unexpected type" in caplog.text
        else:
            assert not caplog.text

    def test_first(self):
        getter = MultiDictGetter([{"a": 1, "b": 2}, {"b": 222, "c": 333}])
        assert getter.first("a") == 1
        assert getter.first("b") == 2
        assert getter.first("c") == 333
        assert getter.first("d") is None
        assert getter.first("d", default=666) == 666

    def test_select(self):
        getter = MultiDictGetter(
            [
                {"a": {"aa": {"aaa": 1, "aab": 2}, "ab": 3}, "b": {"ba": 4, "bb": {"bba": 5}}},
                {"a": {"aa": {"aaa": 10, "aac": 12}, "ac": 13}, "b": {"ba": 14, "bc": {"bbc": 15}}},
            ]
        )
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


def test_common_prefix_empty():
    assert common_prefix([]) == []


def test_common_prefix_single():
    assert common_prefix([[1, 2, 3]]) == [1, 2, 3]


@pytest.mark.parametrize(
    ["first", "second", "expected"],
    [
        ([1, 2, 3, 4], [1, 2, 3, 5], [1, 2, 3]),
        ([1, 2, 3, 4], [2, 3, 4, 1], []),
        ([1, 2, 3], [1, 2, "3"], [1, 2]),
        (range(5), range(4), [0, 1, 2, 3]),
    ],
)
def test_common_prefix_basic(first, second, expected):
    assert common_prefix([first, second]) == expected


def test_common_prefix_multiple():
    assert common_prefix(
        (
            [1, 2, 3, 4],
            (1, 2, 3, 5),
            (x for x in [1, 2, 3, 6]),
            range(1, 9),
        )
    ) == [1, 2, 3]


def test_drop_dict_keys():
    assert drop_dict_keys({}, keys=["foo"]) == {}
    assert drop_dict_keys({"foo": 2, "bar": 3}, keys=["foo"]) == {"bar": 3}
    assert drop_dict_keys([{"foo": 2, "bar": 3}, {"baz": 5}, {"meh": 8}], keys=["foo", "baz"]) == [
        {"bar": 3},
        {},
        {"meh": 8},
    ]
    assert drop_dict_keys(
        {
            "foo": {1: 1, 2: 2, 3: 3},
            "bar": {2: 22, 3: 33, 4: 44},
            "baz": {3: 333, 2: 222, 5: 555},
        },
        keys=[1, 2, 4, "bar"],
    ) == {
        "foo": {3: 3},
        "baz": {3: 333, 5: 555},
    }
    assert drop_dict_keys(
        [
            [{1: 1}, {2: 2, 3: 3}],
            ({3: 3}, {4: 4, 5: 5}),
        ],
        keys=[3, 5],
    ) == [
        [{1: 1}, {2: 2}],
        ({}, {4: 4}),
    ]


def test_drop_dict_keys_copy():
    d = {"foo": 1, "bar": 2}
    res = drop_dict_keys(d, keys=["foo"])
    assert d == {"foo": 1, "bar": 2}
    assert res == {"bar": 2}


def test_is_whitelisted_basic():
    assert is_whitelisted("foo", ["foo", "bar"])
    assert not is_whitelisted("baz", ["foo", "bar"])


def test_is_whitelisted_on_empty():
    assert not is_whitelisted("foo", None)
    assert is_whitelisted("foo", None, on_empty=True)
    assert not is_whitelisted("foo", [])
    assert is_whitelisted("foo", [], on_empty=True)


def test_is_whitelisted_regex():
    assert is_whitelisted("foo", [re.compile("fo*"), "bar"])
    assert not is_whitelisted("foobar", [re.compile("fo*"), "bar"])
    assert is_whitelisted("foobar", [re.compile("f.*"), "bar"])
    assert is_whitelisted("foobar", [re.compile(r"\w+bar"), "bar"])
    assert not is_whitelisted("barfoo", [re.compile("f.*"), "bar"])
    assert is_whitelisted("barfoo", [re.compile(".*f.*"), "bar"])


class TestAttrStatsProxy:
    def test_basic(self):
        class Foo:
            def bar(self, x):
                return x + 1

            def meh(self, x):
                return x * 2

        foo = AttrStatsProxy(target=Foo(), to_track=["bar"])

        assert foo.bar(3) == 4
        assert foo.meh(6) == 12

        assert foo.stats == {"bar": 1}


def test_string_or_regex_match_str():
    assert string_or_regex_match("foo", "foo") is True
    assert string_or_regex_match("foo", "bar") is False


def test_string_or_regex_match_regex():
    assert string_or_regex_match(re.compile("(foo|bar)"), "foo") is True
    assert string_or_regex_match(re.compile("(foo|ba+r)"), "baaar") is True
    assert string_or_regex_match(re.compile("(foo|bar)"), "meh") is False
    assert string_or_regex_match(re.compile("(foo|bar)"), "foobar") is False
    assert string_or_regex_match(re.compile("(foo|bar).*"), "foozuu") is True
    assert string_or_regex_match(re.compile(".*(foo|bar)"), "meebar") is True


def test_string_or_regex_match_invalid():
    with pytest.raises(TypeError, match=re.escape("Invalid pattern [1, 2, 3]")):
        string_or_regex_match([1, 2, 3], "foo")
