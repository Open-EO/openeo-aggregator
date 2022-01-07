import collections
import contextlib
import functools
import logging
import re
import time
from typing import List, Dict, Any, Iterator, Callable, Tuple, Set, Union

import flask
import requests

import openeo_aggregator.about
from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo.rest.auth.auth import BearerAuth, OpenEoApiAuthBase
from openeo_aggregator.config import CACHE_TTL_DEFAULT, CONNECTION_TIMEOUT_DEFAULT, STREAM_CHUNK_SIZE_DEFAULT, \
    AggregatorConfig, CONNECTION_TIMEOUT_INIT
from openeo_aggregator.utils import TtlCache, _UNSET, EventHandler
from openeo_driver.backend import OidcProvider
from openeo_driver.errors import OpenEOApiException, AuthenticationRequiredException, \
    AuthenticationSchemeInvalidException, InternalException

_log = logging.getLogger(__name__)


class LockedAuthException(InternalException):
    def __init__(self):
        super().__init__(message="Setting auth while locked.")


class InvalidatedConnection(InternalException):
    def __init__(self):
        super().__init__(message="Usage of invalidated connection")


class BackendConnection(Connection):
    """
    Aggregator-specific subclass of an `openeo.Connection`, mainly adding these features:

    - a connection has an internal `id` to identify it among multiple backend connections
    - authentication is locked down: only short term authentication is allowed (during lifetime of a flask request)
    """

    # TODO: subclass from RestApiConnection to avoid inheriting feature set
    #       designed for single-user use case (e.g. caching, working local config files, ...)

    def __init__(
            self,
            id: str,
            url: str,
            configured_oidc_providers: List[OidcProvider],
            default_timeout: int = CONNECTION_TIMEOUT_DEFAULT,
            init_timeout: int = CONNECTION_TIMEOUT_INIT,
    ):
        # Temporarily unlock `_auth` for `super().__init__()`
        self._auth_locked = False
        super(BackendConnection, self).__init__(url, default_timeout=init_timeout)
        self._auth = None
        self._auth_locked = True

        self.id = id
        self.default_headers["User-Agent"] = "openeo-aggregator/{v}".format(
            v=openeo_aggregator.about.__version__,
        )
        # Mapping of aggregator provider id to backend's provider id
        self._oidc_provider_map: Dict[str, str] = self._build_oidc_provider_map(configured_oidc_providers)

        self.default_timeout = default_timeout

    def _get_auth(self) -> Union[None, OpenEoApiAuthBase]:
        return None if self._auth_locked else self._auth

    def _set_auth(self, auth: OpenEoApiAuthBase):
        if self._auth_locked:
            raise LockedAuthException
        self._auth = auth

    auth = property(_get_auth, _set_auth)

    def _build_oidc_provider_map(self, configured_providers: List[OidcProvider]) -> Dict[str, str]:
        """Construct mapping from aggregator OIDC provider id to backend OIDC provider id"""
        pid_map = {}
        if configured_providers:
            backend_providers = [
                OidcProvider.from_dict(p)
                for p in self.get("/credentials/oidc", expected_status=200).json()["providers"]
            ]
            for agg_provider in configured_providers:
                targets = [bp.id for bp in backend_providers if bp.get_issuer() == agg_provider.get_issuer()]
                if targets:
                    pid_map[agg_provider.id] = targets[0]
        return pid_map

    @property
    def oidc_provider_map(self) -> Dict[str, str]:
        return self._oidc_provider_map

    def _get_bearer(self, request: flask.Request) -> str:
        """Extract authorization header from request and (optionally) transform for given backend """
        if "Authorization" not in request.headers:
            raise AuthenticationRequiredException
        auth = request.headers["Authorization"]

        if auth.startswith("Bearer basic//"):
            return auth.partition("Bearer ")[2]
        elif auth.startswith("Bearer oidc/"):
            _, pid, token = auth.split("/")
            if pid not in self._oidc_provider_map:
                _log.warning(f"OIDC provider mapping failure: {pid} not in {self._oidc_provider_map}.")
            backend_pid = self._oidc_provider_map.get(pid, pid)
            return f"oidc/{backend_pid}/{token}"
        else:
            raise AuthenticationSchemeInvalidException

    @contextlib.contextmanager
    def authenticated_from_request(self, request: flask.Request):
        """
        Context manager to temporarily authenticate connection based on current flask request.
        """
        self._auth_locked = False
        self.auth = BearerAuth(bearer=self._get_bearer(request=request))
        try:
            yield self
        finally:
            self.auth = None
            self._auth_locked = True

    @contextlib.contextmanager
    def override(self, default_timeout: int = _UNSET, default_headers: dict = _UNSET):
        """
        Context manager to temporarily override default settings of the connection
        """
        # TODO move this to Python client
        orig_default_timeout = self.default_timeout
        orig_default_headers = self.default_headers
        if default_timeout is not _UNSET:
            self.default_timeout = default_timeout
        if default_headers is not _UNSET:
            self.default_headers = default_headers
        yield self
        self.default_timeout = orig_default_timeout
        self.default_headers = orig_default_headers

    def invalidate(self):
        """Destroy connection to avoid accidental usage."""

        def request(*args, **kwargs):
            raise InvalidatedConnection

        self.request = request


_ConnectionsCache = collections.namedtuple("_ConnectionsCache", ["expiry", "connections"])


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """
    # TODO: API version management: just do single/fixed-version federation, or also handle version discovery?
    # TODO: keep track of (recent) backend failures, e.g. to automatically blacklist a backend
    # TODO: synchronized backend connection caching/flushing across gunicorn workers, for better consistency?

    # TODO: move this connections caching ttl to config?
    _CONNECTIONS_CACHING_TTL = 5 * 60

    _TIMEOUT = 5

    # Simplify mocking time for unit tests.
    _clock = time.time  # TODO: centralized helper for this test pattern

    def __init__(self, backends: Dict[str, str], configured_oidc_providers: List[OidcProvider]):
        if any(not re.match(r"^[a-z0-9]+$", bid) for bid in backends.keys()):
            raise ValueError(
                f"Backend ids should be alphanumeric only (no dots, dashes, ...) "
                f"to avoid collision issues when used as prefix. Got: {list(backends.keys())}"
            )
        # TODO: backend_urls as dict does not have explicit order, while this is important.
        self._backend_urls = backends
        self._configured_oidc_providers = configured_oidc_providers

        # General (metadata/status) caching
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT, name="MultiBackendConnection")

        # Caching of connection objects
        self._connections_cache = _ConnectionsCache(expiry=0, connections=[])
        # Event handler for when there is a change in the set of working back-end ids.
        self.on_connections_change = EventHandler("connections_change")
        self.on_connections_change.add(self._cache.flush_all)

    @staticmethod
    def from_config(config: AggregatorConfig) -> 'MultiBackendConnection':
        return MultiBackendConnection(
            backends=config.aggregator_backends,
            configured_oidc_providers=config.configured_oidc_providers
        )

    def _get_connections(self, skip_failures=False) -> Iterator[BackendConnection]:
        """Create new backend connections."""
        for (bid, url) in self._backend_urls.items():
            try:
                _log.info(f"Create backend {bid!r} connection to {url!r}")
                # TODO: Creating connection usually involves version discovery and request of capability doc.
                #       Additional health check necessary?
                yield BackendConnection(id=bid, url=url, configured_oidc_providers=self._configured_oidc_providers)
            except Exception as e:
                _log.warning(f"Failed to create backend {bid!r} connection to {url!r}: {e!r}")
                if not skip_failures:
                    raise

    def get_connections(self) -> List[BackendConnection]:
        """Get backend connections (re-created automatically if cache ttl expired)"""
        now = self._clock()
        if now > self._connections_cache.expiry:
            _log.debug(f"Connections cache expired ({now:.2f}>{self._connections_cache.expiry:.2f})")
            orig_bids = [c.id for c in self._connections_cache.connections]
            for con in self._connections_cache.connections:
                con.invalidate()
            self._connections_cache = _ConnectionsCache(
                expiry=now + self._CONNECTIONS_CACHING_TTL,
                connections=list(self._get_connections(skip_failures=True))
            )
            new_bids = [c.id for c in self._connections_cache.connections]
            _log.debug(
                f"Created {len(self._connections_cache.connections)} actual"
                f" of {len(self._backend_urls)} configured connections"
                f" (TTL {self._CONNECTIONS_CACHING_TTL}s)"
            )
            if orig_bids != new_bids:
                if len(orig_bids) > 0:
                    _log.warning(f"Connections changed {orig_bids} -> {new_bids}: triggering on_connections_change")
                self.on_connections_change.trigger(skip_failures=True)

        return self._connections_cache.connections

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self.get_connections())

    def get_disabled_connection_ids(self) -> Set[str]:
        all_ids = set(self._backend_urls.keys())
        active_ids = set(b.id for b in self.get_connections())
        return all_ids.difference(active_ids)

    def first(self) -> BackendConnection:
        """Get first backend in the list"""
        # TODO: rename this to main_backend (if it makes sense to have a general main backend)?
        return self.get_connections()[0]

    def get_connection(self, backend_id: str) -> BackendConnection:
        for con in self:
            if con.id == backend_id:
                return con
        raise OpenEOApiException(f"No backend with id {backend_id!r}")

    def get_status(self) -> dict:
        return {
            c.id: {
                # TODO: avoid private attributes?
                # TODO: add real backend status? (cached?)
                "root_url": c._root_url,
                "orig_url": c._orig_url,
            }
            for c in self.get_connections()
        }

    def _get_api_version(self) -> ComparableVersion:
        # TODO: ignore patch level of API versions?
        versions = set(v for (i, v) in self.map(lambda c: c.capabilities().api_version()))
        if len(versions) != 1:
            raise OpenEOApiException(f"Only single version is supported, but found: {versions}")
        return ComparableVersion(versions.pop())

    @property
    def api_version(self) -> ComparableVersion:
        return self._cache.get_or_call(key="api_version", callback=self._get_api_version, log_on_miss=True)

    def map(self, callback: Callable[[BackendConnection], Any]) -> Iterator[Tuple[str, Any]]:
        """
        Query each backend connection with given callable and return results as iterator

        :param callback: function to apply to the connection
        """
        for con in self.get_connections():
            res = callback(con)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res

    def get_oidc_providers(self) -> List[OidcProvider]:
        """
        Determine OIDC providers to use in aggregator (based on OIDC issuers supported by all backends)
        and set up provider id mapping in the backend connections

        :param configured_providers: OIDC providers dedicated/configured for the aggregator
        :return: list of actual OIDC providers to use (configured for aggregator and supported by all backends)
        """
        # Get intersection of aggregator OIDC provider ids
        agg_pids_per_backend = [set(c.oidc_provider_map.keys()) for c in self.get_connections()]
        intersection: Set[str] = functools.reduce((lambda x, y: x.intersection(y)), agg_pids_per_backend)
        _log.debug(f"OIDC provider intersection: {intersection}")
        if len(intersection) == 0:
            _log.warning(f"Emtpy OIDC provider intersection. Issuers per backend: {agg_pids_per_backend}")

        # Take configured providers for common issuers.
        agg_providers = [p for p in self._configured_oidc_providers if p.id in intersection]
        _log.info(f"Actual aggregator OIDC providers: {agg_providers}")

        return agg_providers


def streaming_flask_response(
        backend_response: requests.Response,
        chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT
) -> flask.Response:
    """
    Convert a `requests.Response` coming from a backend
    to a (streaming) `flask.Response` to send to the client

    :param backend_response: `requests.Response` object (possibly created with "stream" option enabled)
    :param chunk_size: chunk size to use for streaming
    """
    headers = [
        (k, v) for (k, v) in backend_response.headers.items()
        if k.lower() in ["content-type"]
    ]
    return flask.Response(
        # Streaming response through `iter_content` generator (https://flask.palletsprojects.com/en/2.0.x/patterns/streaming/)
        response=backend_response.iter_content(chunk_size=chunk_size),
        status=backend_response.status_code,
        headers=headers,
    )
