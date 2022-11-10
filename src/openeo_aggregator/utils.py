import datetime
import functools
import logging
import time
from typing import Callable, Iterable, Iterator, List, NamedTuple, Set, Any

import shapely.geometry

from openeo.util import rfc3339

# Generic "sentinel object" for unset values (where `None` is valid value)
# https://python-patterns.guide/python/sentinel-object/)
_UNSET = object()

_log = logging.getLogger(__name__)


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

    def single_value_for(self, key:str)-> Any:
        """Get value for given key and ensure that it is same everywhere"""
        values = set(self.get(key=key))
        if len(values) != 1:
            raise ValueError(f"Single value expected, but got {values}")
        return values.pop()

    def keys(self) -> Set[str]:
        return functools.reduce(lambda a, b: a.union(b), (d.keys() for d in self.dictionaries), set())

    def has_key(self, key: str) -> bool:
        return any(key in d for d in self.dictionaries)

    def available_keys(self, keys: List[str]) -> List[str]:
        return [k for k in keys if self.has_key(k)]

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
                _log.warning(
                    f"MultiDictGetter.concat with {key=}: skipping unexpected type {items}"
                )
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
