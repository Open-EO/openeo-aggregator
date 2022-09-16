import datetime
import logging
import types

import shapely.geometry
import time
from typing import Callable, Iterable, Iterator, List, NamedTuple

from openeo.util import TimingLogger, rfc3339

# Generic "sentinel object" for unset values (where `None` is valid value)
# https://python-patterns.guide/python/sentinel-object/)
_UNSET = object()


class CacheMissException(KeyError):
    pass


_log = logging.getLogger(__name__)


class TtlCache:
    """
    Simple (in-memory) dict-style cache with expiry
    """

    def __init__(self, default_ttl: int = 60, clock: Callable[[], float] = time.time, name: str = None):
        self._cache = {}
        self.default_ttl = default_ttl
        self._clock = clock  # TODO: centralized helper for this test pattern
        self.name = name or "TtlCache"

    def set(self, key, value, ttl=None):
        """Add item to cache"""
        self._cache[key] = (value, self._clock() + (ttl or self.default_ttl))

    def __setitem__(self, key, value):
        """Add item to cache"""
        self.set(key, value)

    def contains(self, key) -> bool:
        """Check whether cache contains item under given key"""
        if key not in self._cache:
            return False
        value, expiration = self._cache[key]
        if self._clock() <= expiration:
            return True
        else:
            del self._cache[key]
            return False

    def __contains__(self, key):
        """Check whether cache contains item under given key"""
        return self.contains(key)

    def get(self, key, default=None):
        """Get item from cache and if not available: return default value."""
        return self._cache[key][0] if self.contains(key) else default

    def __getitem__(self, key):
        """Get item from cache and raise `CacheMissException` if not available."""
        if self.contains(key):
            return self._cache[key][0]
        raise CacheMissException(key)

    def get_or_call(self, key, callback, ttl=None, log_on_miss=False):
        """
        Helper to compactly implement the "get from cache or calculate otherwise" pattern
        """
        if self.contains(key):
            res = self[key]
        else:
            if log_on_miss:
                with TimingLogger(
                        title=f"Cache miss {self.name!r} key {key!r}, calling {callback.__qualname__!r}",
                        logger=_log.debug
                ):
                    res = callback()
            else:
                res = callback()
            self.set(key, res, ttl=ttl)
        return res

    def flush_all(self):
        self._cache = {}


class MultiDictGetter:
    """
    Helper to get (and combine) items (where available) from a collection of dictionaries.
    """

    def __init__(self, dictionaries: Iterable[dict]):
        self.dictionaries = list(dictionaries)

    def get(self, key: str) -> Iterator:
        for d in self.dictionaries:
            if key in d:
                yield d[key]

    def concat(self, key: str, skip_duplicates=False) -> list:
        """
        Concatenate all lists/tuples at given `key (optionally skipping duplicate items in the process)
        """
        result = []
        for items in self.get(key):
            if isinstance(items, (list, tuple)):
                for item in items:
                    if skip_duplicates and item in result:
                        continue
                    result.append(item)
            else:
                _log.warning(f"Skipping unexpected type in MultiDictGetter.concat: {items}")
        return result

    def union(self, key:str) -> set:
        """Like `concat` but with set-wise union (removing duplicates)."""
        return set(self.concat(key=key, skip_duplicates=True))

    def simple_merge(self, included_keys=None) -> dict:
        """
        All dictionaries are merged following simple rules:
        For list or sets: all elements are merged into a single list, without duplicates.
        For dictionaries: all keys are added to a single dict, duplicate keys are merged recursively.
        For all other types: the first value is returned.

        It assumes that all duplicate keys in a dictionary have items of the same type.

        Args:
            included_keys: If given, only these top level keys are merged into the final dict.
        """
        if len(self.dictionaries) == 0:
            return {}
        if len(self.dictionaries) == 1:
            return self.dictionaries[0]

        result = {}
        for dictionary in self.dictionaries:
            for key, item in dictionary.items():
                if included_keys is not None and key not in included_keys:
                    continue
                if key in result:
                    if isinstance(item, list) or isinstance(item, set):
                        result[key] = self.concat(key, skip_duplicates=True)
                    elif isinstance(item, dict):
                        result[key] = self.select(key).simple_merge()
                else:
                    result[key] = item
        return result

    def first(self, key, default=None):
        return next(self.get(key), default)

    def select(self, key: str) -> 'MultiDictGetter':
        """Create new getter, one step deeper in the dictionary hierarchy."""
        return MultiDictGetter(d for d in self.get(key=key) if isinstance(d, dict))


def subdict(d: dict, *args, keys: Iterable[str] = None, default=None) -> dict:
    """Extract dict with only selected keys from given dict"""
    keys = set(keys or []) | set(args)
    # TODO: way to not provide default and raise KeyError on missing keys
    # TODO: move to openeo-python-driver?
    return {k: d.get(k, default) for k in keys}


def dict_merge(*args, **kwargs) -> dict:
    """
    Helper to merge dictionaries.

    Creates new dictionary, input dictionary are left untouched.
    Priority of items increases to the right (with highest priority for keyword arguments).
    """
    # TODO: move this to upstream dependency (openeo package)?
    result = {}
    for d in args + (kwargs,):
        result.update(d)
    return result


class EventHandler:
    """Simple event handler that allows to collect callbacks to call on a certain event."""

    def __init__(self, name: str):
        self._name = name
        self._callbacks: List[Callable[[], None]] = []

    def add(self, callback: Callable[[], None]):
        """Add a callback to call when the event is triggered."""
        self._callbacks.append(callback)

    def trigger(self, skip_failures=False):
        """Call all callbacks."""
        _log.info(f"Triggering event {self._name!r}")
        for callback in self._callbacks:
            try:
                callback()
            except Exception as e:
                _log.error(f"Failure calling event {self._name!r} callback {callback!r}: {e}")
                if not skip_failures:
                    raise


class Clock:
    """
    Time/date helper, allowing overrides of "current" time/date for test purposes.
    """

    # TODO: start using a dedicated time mocking tool like freezegun (https://github.com/spulec/freezegun)
    #       or time-machine (https://github.com/adamchainz/time-machine)?

    _time = time.time

    @classmethod
    def time(cls) -> float:
        """
        Like `time.time()`: current time as Unix Epoch timestamp.
        """
        return cls._time()

    @classmethod
    def utcnow(cls) -> datetime.datetime:
        """
        Like `datetime.datetime.utcnow()`: Current UTC datetime (naive).
        """
        return datetime.datetime.utcfromtimestamp(cls.time())


class BoundingBox(NamedTuple):
    """Simple NamedTuple container for a bounding box """
    # TODO: move this to openeo_driver
    west: float
    south: float
    east: float
    north: float
    # TODO: also accept integer EPSG code as CRS?
    # TODO: automatically normalize CRS (e.g. lower case)
    crs: str = "EPSG:4326"

    @classmethod
    def from_dict(cls, d: dict) -> "BoundingBox":
        return cls(**{
            k: d[k]
            for k in cls._fields
            if k not in cls._field_defaults or k in d
        })

    def as_dict(self) -> dict:
        return self._asdict()

    def as_polygon(self) -> shapely.geometry.Polygon:
        """Get bounding box as a shapely Polygon"""
        return shapely.geometry.box(minx=self.west, miny=self.south, maxx=self.east, maxy=self.north)

    def contains(self, x: float, y: float) -> bool:
        """Check if given point is inside the bounding box"""
        return (self.west <= x <= self.east) and (self.south <= y <= self.north)


def strip_join(separator: str, *args: str) -> str:
    """
    Join multiple strings with given separator,
    but avoid repeated separators by first stripping it from the glue points
    """
    if len(args) > 1:
        args = [args[0].rstrip(separator)] + [a.strip(separator) for a in args[1:-1]] + [args[-1].lstrip(separator)]
    return separator.join(args)


def timestamp_to_rfc3339(timestamp: float) -> str:
    """Convert unix epoch timestamp to RFC3339 datetime string"""
    dt = datetime.datetime.utcfromtimestamp(timestamp)
    return rfc3339.datetime(dt)


def normalize_issuer_url(url: str) -> str:
    return url.rstrip("/").lower()
