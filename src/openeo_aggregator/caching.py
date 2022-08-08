import abc
import contextlib
import functools
import json
import logging
import time
from typing import Callable, Union, Optional, Sequence, Any, Tuple, List

import kazoo.exceptions
import kazoo.protocol.paths
from kazoo.client import KazooClient

from openeo.util import TimingLogger
from openeo_aggregator.config import AggregatorConfig
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


# Typehint for cache keys: single string or tuple/list of strings (e.g. path components, to be joined in some way)
CacheKey = Union[str, Sequence[str]]


class Memoizer(metaclass=abc.ABCMeta):
    """
    (Abstract) base class for function call caching (memoization).

    Implementing classes should just provide `get_or_call` implementation.
    """
    log_on_miss = True

    @abc.abstractmethod
    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        return NotImplementedError

    def wrap(self, key: CacheKey, ttl: Optional[float]):
        """Wrapper to use memoizer as function/method decorator."""

        def wrapper(callback: Callable[[], Any]):
            @functools.wraps(callable)
            def wrapped():
                return self.get_or_call(key=key, callback=callback, ttl=ttl)

            return wrapped

        return wrapper

    def _normalize_key(self, key: CacheKey) -> Tuple[str, ...]:
        """Normalize a cache key to tuple"""
        if isinstance(key, str):
            key = (key,)
        return tuple(key)

    def _wrap_logging(self, callback: Callable[[], Any], key) -> Callable:
        """Wrap given callable with (timing) logging"""
        if self.log_on_miss:
            title = f"Cache miss {self!r} key {key!r}: calling {callback!r}"
            logger = self.log_on_miss if isinstance(self.log_on_miss, logging.Logger) else _log.debug
            timing_logger = TimingLogger(title=title, logger=logger)
            # Use decorator functionality of TimingLogger to wrap the callback:
            callback = timing_logger(callback)
        return callback

    def flush_all(self):
        # TODO: implement `flush_all` here or avoid requiring `flush_all` in the first place?
        _log.warning(f"Cache {self!r} not doing flush_all for now")


class NullMemoizer(Memoizer):
    """No caching at all."""

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        callback = self._wrap_logging(callback=callback, key=key)
        return callback()


class DictMemoizer(Memoizer):
    """Simple memoization with an in-memory dictionary"""

    # TODO: this memoizer replaces basically TtlCache, remove the latter?

    DEFAULT_TTL = 60

    def __init__(self, default_ttl: Optional[float] = None):
        self._cache = {}
        self._default_ttl = float(default_ttl or self.DEFAULT_TTL)

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        key = self._normalize_key(key)
        ttl = ttl or self._default_ttl
        try:
            value, ts = self._cache[key]
            if ts + ttl <= Clock.time():
                del self._cache[key]
                raise CacheExpiredException
        except (KeyError, CacheExpiredException):
            callback = self._wrap_logging(callback=callback, key=key)
            value = callback()
            self._cache[key] = (value, Clock.time())
        return value

    def flush_all(self):
        self._cache = {}


class ChainedMemoizer(Memoizer):
    """Chain multiple memoizers for multilevel caching."""

    def __init__(self, memoizers: List[Memoizer]):
        self._memoizers = memoizers

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        # Build chained callback function by iteratively wrapping callback in memoizer wrappers
        # (from the deepest level to top-level)
        for memoizer in self._memoizers[::-1]:
            callback = memoizer.wrap(key=key, ttl=ttl)(callback)
        return callback()


@contextlib.contextmanager
def zk_connected(client: KazooClient, timeout: float = 5) -> KazooClient:
    """
    Context manager to automatically start and stop ZooKeeper connection.
    """
    client.start(timeout=timeout)
    try:
        yield client
    finally:
        client.stop()


class ZkMemoizer(Memoizer):
    """
    ZooKeeper based caching of function call results.

    Basic usage:

        zk_cache = ZkMemoizer(zk_client, prefix="/myapp/cache/mysubsystem")
        count = zk_cache.get_or_call("count", callback=calculate_count)

    """
    DEFAULT_TTL = 5 * 60
    DEFAULT_ZK_TIMEOUT = 5

    def __init__(
            self,
            client: KazooClient,
            prefix: str,
            default_ttl: Optional[float] = None,
            zk_timeout: Optional[float] = None,
    ):
        self._client = client
        self._prefix = kazoo.protocol.paths.normpath(prefix)
        self._default_ttl = float(default_ttl or self.DEFAULT_TTL)
        self._zk_timeout = float(zk_timeout or self.DEFAULT_ZK_TIMEOUT)

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """
        Try to get data from cache or calculate otherwise
        """

        path = self._path(key)
        ttl = ttl or self._default_ttl
        callback = self._wrap_logging(callback=callback, key=key)

        try:
            with zk_connected(self._client, timeout=self._zk_timeout) as connected_client:
                # Let's see if we find something in cache.
                try:
                    zk_value, zk_stat = connected_client.get(path=path)
                except kazoo.exceptions.NoNodeError:
                    # Simple cache miss: no zookeeper node, so we'll create it.
                    value = callback()
                    self._try_create(connected_client=connected_client, path=path, value=value)
                    return value
                # We found something, let's see if we can read it
                try:
                    value = self._deserialize(zk_value)
                except json.JSONDecodeError as e:
                    # Cache hit, but corrupt data: update it
                    _log.error(f"{self!r} key {key!r} corrupted data: {e!r}")
                    value = callback()
                    self._try_set(connected_client=connected_client, path=path, value=value)
                    return value
                # Expiry logic: note that we evaluate the TTL at "get time",
                # instead of storing an expiry threshold at "set time" along with the data
                expiry = zk_stat.last_modified + ttl
                now = Clock.time()
                if expiry <= now:
                    _log.debug(f"{self!r} key {key!r} expired ({expiry} < {now})")
                    value = callback()
                    self._try_set(connected_client=connected_client, path=path, value=value)
                return value
        except Exception as e:
            _log.error(f"{self!r} key {key!r} failure: {e!r}")
            value = callback()
        return value

    def _path(self, key: CacheKey) -> str:
        """Helper to build a zookeeper path"""
        key = self._normalize_key(key)
        path = strip_join("/", "", self._prefix, *key)
        return kazoo.protocol.paths.normpath(path)

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

    @classmethod
    def _try_create(cls, connected_client, path, value):
        """Try to create cache node (zk node does not exist yet)"""
        try:
            connected_client.create(path=path, value=cls._serialize(value), makepath=True)
        except kazoo.exceptions.NodeExistsError:
            # Node has been set in the meantime by other worker probably
            _log.warning(f"Tried to create new zk cache node {path!r}, but it already exists.")
        except Exception as e:
            _log.error(f"Failed to create new zk cache node {path!r}: {e!r}")
            # Continue without setting cache

    @classmethod
    def _try_set(cls, connected_client, path, value):
        """Try to set/update cache node (zk node must exist already)"""
        try:
            connected_client.set(path=path, value=cls._serialize(value))
        except Exception as e:
            _log.error(f"Failed to update zk cache node {path!r}: {e!r}")
            # Continue without setting cache


def memoizer_from_config(
        config: AggregatorConfig,
        namespace: str,
) -> Memoizer:
    """Factory to create `ZkMemoizer` instance from config values."""
    memoizer_type = config.memoizer.get("type", "null")
    memoizer_conf = config.memoizer.get("config", {})
    if memoizer_type == "null":
        return NullMemoizer()
    elif memoizer_type == "dict":
        return DictMemoizer(default_ttl=memoizer_conf.get("default_ttl"))
    elif memoizer_type == "zookeeper":
        return ZkMemoizer(
            client=KazooClient(hosts=memoizer_conf.get("zk_hosts", "localhost:2181")),
            prefix=f"{config.zookeeper_prefix}/cache/{namespace}",
            default_ttl=memoizer_conf.get("default_ttl"),
            zk_timeout=memoizer_conf.get("zk_timeout"),
        )
    else:
        raise ValueError(memoizer_type)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    zk_client = KazooClient(hosts="127.0.0.1:2181")
    zk_cache = ZkMemoizer(client=zk_client, prefix="openeo/aggregator/tmp/cache")

    import datetime
    import os
    import random

    data = {
        "pid": os.getpid(),
        "random": random.randrange(100, 999),
        "now": datetime.datetime.now().isoformat()
    }
    print("New data: ", data)

    res = zk_cache.get_or_call(key="test", callback=(lambda: data), ttl=10)
    print("Got data: ", res)
