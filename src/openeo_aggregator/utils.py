import logging
import time
from typing import Callable, Iterable, Iterator, List

# Generic "sentinel object" for unset values (where `None` is valid value)
# https://python-patterns.guide/python/sentinel-object/)
_UNSET = object()


class CacheMissException(KeyError):
    pass


_log = logging.getLogger(__name__)


class TtlCache:
    """
    Simple dict-style cache with expiry
    """

    def __init__(self, default_ttl: int = 60, clock: Callable[[], float] = time.time):
        self._cache = {}
        self.default_ttl = default_ttl
        self._clock = clock

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

    def get_or_call(self, key, callback, ttl=None):
        """
        Helper to compactly implement the "get from cache or calculate otherwise" pattern
        """
        if self.contains(key):
            res = self[key]
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


def subdict(d: dict, *args, keys: list = None, default=None) -> dict:
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
