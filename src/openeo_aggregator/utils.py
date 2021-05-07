import time
from typing import Callable
import logging


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
        self._cache[key] = (value, self._clock() + (ttl or self.default_ttl))

    def __setitem__(self, key, value):
        self.set(key, value)

    def contains(self, key):
        if key not in self._cache:
            return False
        value, expiration = self._cache[key]
        if self._clock() <= expiration:
            return True
        else:
            del self._cache[key]
            return False

    def __contains__(self, key):
        return self.contains(key)

    def get(self, key, default=None):
        return self._cache[key][0] if self.contains(key) else default

    def __getitem__(self, key):
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
