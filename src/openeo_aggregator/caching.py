import contextlib
import json
import logging
import time
from typing import Callable, Tuple, Union

import kazoo.exceptions
import kazoo.protocol.paths
from kazoo.client import KazooClient

from openeo.util import TimingLogger
from openeo_aggregator.utils import strip_join, Clock


class CacheMissException(KeyError):
    pass


class CacheExpiredException(CacheMissException):
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


class ZkMemoizer:
    """
    ZooKeeper based caching of function call results.

    Basic usage:

        >>> zk_cache = ZkMemoizer(zk_client, prefix="/myapp/cache/mysubsystem")
        >>> count = zk_cache.get_or_call("count", callback=calculate_count)

    """

    def __init__(self, client: KazooClient, prefix: str, default_ttl: int = 60, name: str = None):
        self._client = client
        self._prefix = prefix
        self._default_ttl = default_ttl
        self._name = name or self.__class__.__name__

    def get_or_call(
            self,
            key: Union[str, Tuple],
            callback: Callable,
            ttl=None,
            log_on_miss=False,
    ):
        """
        Try to get data from cache or calculate otherwise
        """

        path = self._path(key)
        if log_on_miss:
            timing_logger = TimingLogger(
                title=f"Cache miss {self._name!r} key {key!r}: calling {callback.__qualname__!r}",
                logger=_log.debug
            )
            # Use decorator functionality of TimingLogger to wrap the callback
            callback = timing_logger(callback)

        try:
            with self._connect():
                try:
                    zk_value, zk_stat = self._client.get(path=path)
                    value = self._deserialize(zk_value)
                    # Expiry logic: note that we evaluate the TTL at "get time",
                    # instead of storing an expiry threshold at "set time" along with the data
                    expiry = zk_stat.last_modified + (ttl or self._default_ttl)
                    now = Clock.time()
                    if expiry <= now:
                        _log.debug(f"Cache {self._name!r} key {key!r} expired ({expiry} < {now})")
                        raise CacheExpiredException

                except kazoo.exceptions.NoNodeError:
                    # Simple cache miss: no zookeeper node, so we'll create it.
                    value = callback()
                    try:
                        self._client.create(path=path, value=self._serialize(value), makepath=True)
                    except kazoo.exceptions.NodeExistsError:
                        # Node has been set in the meantime by other worker probably
                        _log.warning(f"Tried to create new zk cache node {path!r}, but it already exists.")
                    except Exception as e:
                        _log.error(f"Failed to create new zk cache node {path!r}: {e!r}")
                        # Continue without setting cache
                except (CacheExpiredException, json.JSONDecodeError):
                    # Cache hit, but it expired or is invalid: update it
                    value = callback()
                    try:
                        self._client.set(path=path, value=self._serialize(value))
                    except Exception as e:
                        _log.error(f"Failed to update zk cache node {path!r}: {e!r}")
                        # Continue without setting cache
        except Exception as e:
            _log.error(f"Failed to get zk cache node {path!r}: {e!r}")
            value = callback()
            # TODO: still try to create/set cache in some cases?

        return value

    def _path(self, key: Union[str, Tuple]) -> str:
        """Helper to build a zookeeper path"""
        if isinstance(key, str):
            key = (key,)
        path = strip_join("/", "", self._prefix, *key)
        return kazoo.protocol.paths.normpath(path)

    @contextlib.contextmanager
    def _connect(self):
        """
        Context manager to automatically start and stop zookeeper connection.
        """
        self._client.start(timeout=5)
        try:
            yield self._client
        finally:
            self._client.stop()

    @staticmethod
    def _serialize(value: dict) -> bytes:
        """Serialize a dictionary (given as arguments) in JSON (UTF8 byte-encoded)."""
        # TODO: support other serialization (pickle, json+gzip, ...)?
        # TODO: JSON serialization converts tuples to lists: is that a problem somewhere?
        return json.dumps(value, indent=None, separators=(',', ':')).encode("utf8")

    @staticmethod
    def _deserialize(zk_value: bytes) -> dict:
        """Deserialize bytes (assuming UTF8 encoded JSON mapping)"""
        return json.loads(zk_value.decode("utf8"))
