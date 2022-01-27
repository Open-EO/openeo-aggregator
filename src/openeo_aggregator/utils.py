import datetime
import logging
import time
from typing import Callable, Iterable, Iterator, List, NamedTuple, Union

import shapely.geometry

from openeo.util import TimingLogger

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

    def union(self, key: str, skip_duplicates=False) -> list:
        """
        Simple list based union of the items
        (each of which must be an iterable itself, such as list or set) at given key.
        """
        result = []
        for items in self.get(key):
            for item in items:
                if skip_duplicates and item in result:
                    continue
                result.append(item)
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
