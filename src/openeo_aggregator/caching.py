import abc
import contextlib
import functools
import json
import logging
import time
import zlib
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

import kazoo.exceptions
import kazoo.protocol.paths
from kazoo.client import KazooClient
from openeo.util import TimingLogger

from openeo_aggregator.config import get_backend_config
from openeo_aggregator.utils import AttrStatsProxy, Clock, strip_join

DEFAULT_NAMESPACE = "_default"


class CacheException(Exception):
    pass


class CacheMissException(CacheException):
    pass


class CacheInvalidException(CacheException):
    pass


_log = logging.getLogger(__name__)

UNSET = object()


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
                    title=f"Cache miss {self.name!r} key {key!r}, calling {callback.__qualname__!r}", logger=_log.debug
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
    (Abstract) base class for function call caching (memoization)
    with this usage pattern (get from cache or calculate and cache that result):

        value = memoizer.get_or_call(
            key="foo",
            callback=calculate_foo
        )

    Concrete classes should just implement `get_or_call` and `invalidate`.
    """

    log_on_miss = True

    def __init__(self, namespace: str = DEFAULT_NAMESPACE):
        self._namespace = namespace

    @abc.abstractmethod
    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any: ...

    @abc.abstractmethod
    def invalidate(self): ...

    def wrap(self, key: CacheKey, ttl: Optional[float] = None):
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
            title = f"{self!r} cache miss on key {key!r}. Calling {callback!r}"
            logger = self.log_on_miss if isinstance(self.log_on_miss, logging.Logger) else _log.debug
            timing_logger = TimingLogger(title=title, logger=logger)
            # Use decorator functionality of TimingLogger to wrap the callback:
            callback = timing_logger(callback)
        return callback

    def __repr__(self):
        return f"<{self.__class__.__name__} ns={self._namespace!r}>"


class NullMemoizer(Memoizer):
    """No caching at all."""

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        callback = self._wrap_logging(callback=callback, key=key)
        return callback()

    def invalidate(self):
        pass


class NoSerDe:
    """No serialization"""

    @staticmethod
    def serialize(value: Any) -> Any:
        return value

    @staticmethod
    def deserialize(value: Any) -> Any:
        return value


class JsonSerDe:
    """JSON serialization/deserialization"""

    # TODO: JSON serialization converts tuples to lists: is that a problem somewhere?

    def __init__(self, gzip_threshold: int = 100_000):
        # Supported types with custom encoding
        self._custom_types: Set[type] = set()
        # Mapping of type identifier to load callable
        self._decode_map: Dict[str, Callable] = {}
        self._gzip_threshold = gzip_threshold

    def register_custom_codec(self, tp: type):
        """
        Register a class for custom JSON encoding/decoding.

        class must have:

            - `__jsonserde_prepare__(self)` method to export data to JSON encodable format (e.g. a dict)
            - `__jsonserde_load__(data: dict)` class method to load an instance from a dict (decoded JSON data)

        Can be used as decorator.

        :param tp: class with  `__jsonserde_prepare__(self)` method and `__jsonserde_load__(data: dict)` class method
        :return: the class
        """
        if hasattr(tp, "__jsonserde_prepare__") and hasattr(tp, "__jsonserde_load__"):
            self._custom_types.add(tp)
            self._decode_map[self._type_id(tp)] = tp.__jsonserde_load__
        else:
            raise ValueError(f"{tp!r} must have `__jsonserde_prepare__` method and `__jsonserde_load__` classmethod.")
        return tp

    def _type_id(self, tp: type) -> str:
        """Class identifier as string."""
        return f"{tp.__module__}.{tp.__qualname__}"

    def _default(self, o: Any) -> dict:
        """Implementation of `default` parameter of `json.dump` and related"""
        if o.__class__ in self._custom_types:
            # TODO: also add signing with a secret?
            return {"_jsonserde": {"type": self._type_id(o.__class__), "data": o.__jsonserde_prepare__()}}
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    def _object_hook(self, d: dict) -> Any:
        """Implementation of `object_hook` parameter of `json.load` and related"""
        decode_data = d.get("_jsonserde")
        if decode_data and "type" in decode_data and "data" in decode_data:
            return self._decode_map[decode_data["type"]](decode_data["data"])
        return d

    def serialize(self, data: dict) -> bytes:
        data = json.dumps(
            obj=data,
            indent=None,
            separators=(",", ":"),
            default=self._default if self._custom_types else None,
        ).encode("utf8")
        if len(data) > self._gzip_threshold:
            _log.debug(f"JsonSerDe.serialize: large data payload ({len(data)}): using additional zlib compression")
            data = zlib.compress(data)
        return data

    def deserialize(self, data: bytes) -> dict:
        if data[:1] == b"\x78":
            # First byte of zlib data is practically almost always x78
            _log.debug(f"JsonSerDe.deserialize: detected zlib compressed data")
            data = zlib.decompress(data)
        return json.loads(s=data.decode("utf8"), object_hook=self._object_hook if self._decode_map else None)


# Global JSON SerDe instance
json_serde = JsonSerDe()


class DictMemoizer(Memoizer):
    """Simple memoization with an in-memory dictionary"""

    # TODO: this memoizer replaces basically TtlCache, remove the latter?

    DEFAULT_TTL = 60

    _serde = NoSerDe

    def __init__(self, namespace: str = DEFAULT_NAMESPACE, default_ttl: Optional[float] = None):
        super().__init__(namespace=namespace)
        self._cache = {}
        self._default_ttl = float(default_ttl or self.DEFAULT_TTL)

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        key = self._normalize_key(key)
        ttl = ttl or self._default_ttl

        def _calculate_and_cache(reason: str) -> Any:
            _log.debug(f"{self!r} cache miss: {reason} key {key}")
            value = self._wrap_logging(callback=callback, key=key)()
            try:
                self._cache[key] = (self._serde.serialize(value), Clock.time())
            except Exception as e:
                _log.error(f"{self!r} failed to memoize for {key}: {e!r}")
            return value

        try:
            value, ts = self._cache[key]
        except KeyError:
            # Cache miss
            return _calculate_and_cache(reason="missing")
        try:
            value = self._serde.deserialize(value)
        except json.JSONDecodeError as e:
            _log.error(f"{self!r} key {key} corrupted data: {e!r}")
            return _calculate_and_cache(reason="corrupt")
        if ts + ttl <= Clock.time():
            # Cache expired
            del self._cache[key]
            return _calculate_and_cache(reason="expired")
        _log.debug(f"{self!r} cache hit key {key}")
        return value

    def invalidate(self):
        _log.debug(f"{self!r} invalidate")
        self._cache = {}

    def dump(self, values_only=False) -> Union[dict, list]:
        """Allow inspection of cache for testing purposes"""
        if values_only:
            return [x[0] for x in self._cache.values()]
        else:
            return self._cache


class JsonDictMemoizer(DictMemoizer):
    """In-memory dict memoizer, but with JSON serialization (mainly for testing serialization flows)"""

    _serde = json_serde


class ChainedMemoizer(Memoizer):
    """Chain multiple memoizers for multilevel caching."""

    def __init__(self, memoizers: List[Memoizer], namespace: str = DEFAULT_NAMESPACE):
        super().__init__(namespace=namespace)
        self._memoizers = memoizers

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        # Build chained callback function by iteratively wrapping callback in memoizer wrappers
        # (from the deepest level to top-level)
        # TODO: pre-wrap the memoizers in __init__
        for memoizer in self._memoizers[::-1]:
            callback = memoizer.wrap(key=key, ttl=ttl)(callback)
        return callback()

    def invalidate(self):
        for memoizer in self._memoizers:
            memoizer.invalidate()


class ZkMemoizer(Memoizer):
    """
    ZooKeeper based caching of function call results.

    Basic usage:

        zk_cache = ZkMemoizer(zk_client, prefix="/myapp/cache/mysubsystem")
        count = zk_cache.get_or_call("count", callback=calculate_count)

    """

    DEFAULT_TTL = 5 * 60
    DEFAULT_ZK_TIMEOUT = 5

    _serde = json_serde

    def __init__(
        self,
        client: KazooClient,
        path_prefix: str,
        namespace: str = DEFAULT_NAMESPACE,
        default_ttl: Optional[float] = None,
        zk_timeout: Optional[float] = None,
    ):
        super().__init__(namespace=namespace)
        self._client = client
        self._prefix = kazoo.protocol.paths.normpath(path_prefix)
        self._default_ttl = float(default_ttl or self.DEFAULT_TTL)
        self._zk_timeout = float(zk_timeout or self.DEFAULT_ZK_TIMEOUT)
        # Minimum timestamp for valid entries
        # TODO: parameterize this, e.g. to always accept value from cache?
        self._valid_threshold = Clock.time()
        _log.info(f"Created {self!r} with prefix={self._prefix!r} default_ttl={self._default_ttl}")

    def get_or_call(self, key: CacheKey, callback: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """
        Try to get data from cache or calculate otherwise.

        Note that we proactively swallow errors related to ZooKeeper storage
        in order to keep things working when ZooKeeper is acting up
        (at the cost of reduced caching performance)
        """

        path = self._path(key)
        ttl = ttl or self._default_ttl
        callback = self._wrap_logging(callback=callback, key=key)

        with self._zk_connect_or_not() as connected_client:

            def handle(found: Any = UNSET, store: str = None, debug=None, error=None) -> Any:
                """Helper to compactly handle various cache miss/hit/error situations"""
                if error:
                    _log.error(f"{self!r} {error}")
                elif debug:
                    _log.debug(f"{self!r} {debug}")

                if found is not UNSET:
                    value = found
                else:
                    value = callback()
                    try:
                        # Try to store value (but skip any failure)
                        if store == "create":
                            serialized = self._serde.serialize(value)
                            _log.debug(f"{self!r} serialized size: {len(serialized)}")
                            connected_client.create(path=path, value=serialized, makepath=True)
                        elif store == "set":
                            serialized = self._serde.serialize(value)
                            _log.debug(f"{self!r} serialized size: {len(serialized)}")
                            connected_client.set(path=path, value=serialized)
                    except kazoo.exceptions.NodeExistsError:
                        # When creating node that already exists: another worker probably set cache in the meantime
                        _log.warning(f"{self!r} failed to create node {path!r}: already exists.")
                    except Exception as e:
                        _log.error(f"{self!r} failed to {store} path {path!r}: {e!r}")

                return value

            if connected_client is None:
                return handle(store=None, error="no connection")
            # Let's see if we find something in cache.
            try:
                zk_value, zk_stat = connected_client.get(path=path)
            except kazoo.exceptions.NoNodeError:
                # Simple cache miss: no zookeeper node, so we'll have to create it.
                return handle(store="create", debug=f"cache miss path {path!r}")
            except Exception as e:
                return handle(store=None, error=f"unexpected get failure: {e!r}")

            # We found something, check expiry and validity
            if zk_stat.last_modified < self._valid_threshold:
                return handle(store="set", debug=f"invalidated path {path!r}")
            if zk_stat.last_modified + ttl <= Clock.time():
                # Note that we evaluate the TTL at "get time",
                # instead of storing an expiry threshold at "set time" along with the data
                return handle(store="set", debug=f"expired path {path!r}")
            try:
                # Can we read it?
                value = self._serde.deserialize(zk_value)
            except Exception as e:
                # Cache hit, but corrupt data: update it
                return handle(store="set", error=f"corrupt data path {path!r}: {e!r}")

            return handle(found=value, store=None, debug=f"cache hit path {path!r}")

    def invalidate(self):
        # TODO: this invalidates zk cache data for current ZkMemoizer only
        #   how to signal this to other workers?
        #   Remove zk subtree instead of just setting timestamp threshold?
        _log.debug(f"{self!r} invalidate")
        self._valid_threshold = Clock.time()

    def _path(self, key: CacheKey) -> str:
        """Helper to build a zookeeper path"""
        key = self._normalize_key(key)
        path = strip_join("/", "", self._prefix, *key)
        return kazoo.protocol.paths.normpath(path)

    @contextlib.contextmanager
    def _zk_connect_or_not(self) -> Union[KazooClient, None]:
        """
        Helper context manager to robustly start and stop ZooKeeper connection.
        Swallows start/stop failures (just returns None instead of connected client on start failure).
        """
        # Do nothing if already connected (e.g. an inner context)
        do_start_stop = not self._client.connected
        client = self._client
        try:
            if do_start_stop:
                client.start(timeout=self._zk_timeout)
        except Exception as e:
            _log.error(f"{self!r} failed to start connection: {e!r}")
            client = None
        try:
            yield client
        finally:
            if client and do_start_stop:
                try:
                    client.stop()
                except Exception as e:
                    _log.error(f"{self!r} failed to stop connection: {e!r}")


zk_memoizer_stats = {}


def memoizer_from_config(namespace: str) -> Memoizer:
    """Factory to create `ZkMemoizer` instance from config values."""

    backend_config = get_backend_config()

    def get_memoizer(memoizer_type: str, memoizer_conf: dict) -> Memoizer:
        if memoizer_type == "null":
            return NullMemoizer(namespace=namespace)
        elif memoizer_type == "dict":
            return DictMemoizer(namespace=namespace, default_ttl=memoizer_conf.get("default_ttl"))
        elif memoizer_type == "jsondict":
            return JsonDictMemoizer(namespace=namespace, default_ttl=memoizer_conf.get("default_ttl"))
        elif memoizer_type == "zookeeper":
            kazoo_client = KazooClient(hosts=memoizer_conf.get("zk_hosts", "localhost:2181"))
            if backend_config.zk_memoizer_tracking:
                kazoo_client = AttrStatsProxy(
                    target=kazoo_client,
                    to_track=["start", "stop", "create", "get", "set"],
                    # TODO: better solution than using a module level global here?
                    stats=zk_memoizer_stats,
                )
            zookeeper_prefix = backend_config.zookeeper_prefix
            return ZkMemoizer(
                client=kazoo_client,
                path_prefix=f"{zookeeper_prefix}/cache/{namespace}",
                namespace=namespace,
                default_ttl=memoizer_conf.get("default_ttl"),
                zk_timeout=memoizer_conf.get("zk_timeout"),
            )
        elif memoizer_type == "chained":
            return ChainedMemoizer(
                [
                    get_memoizer(memoizer_type=part["type"], memoizer_conf=part["config"])
                    for part in memoizer_conf["parts"]
                ],
                namespace=namespace,
            )
        else:
            raise ValueError(memoizer_type)

    return get_memoizer(
        memoizer_type=backend_config.memoizer.get("type", "null"),
        memoizer_conf=backend_config.memoizer.get("config", {}),
    )
