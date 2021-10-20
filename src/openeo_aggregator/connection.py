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
from openeo_aggregator.config import CACHE_TTL_DEFAULT, CONNECTION_TIMEOUT_DEFAULT, STREAM_CHUNK_SIZE_DEFAULT
from openeo_aggregator.utils import TtlCache, _UNSET
from openeo_driver.backend import OidcProvider
from openeo_driver.errors import OpenEOApiException, AuthenticationRequiredException, \
    AuthenticationSchemeInvalidException, InternalException

_log = logging.getLogger(__name__)


class LockedAuthException(InternalException):
    """Implementation tries to do permanent authentication on connection"""


class BackendConnection(Connection):
    """
    Aggregator-specific subclass of an `openeo.Connection`, mainly adding these features:

    - a connection has an internal `id` to identify it among multiple backend connections
    - authentication is locked down: only short term authentication is allowed (during lifetime of a flask request)
    """

    # TODO: subclass from RestApiConnection to avoid inheriting feature set
    #       designed for single-user use case (e.g. caching, working local config files, ...)

    def __init__(self, id: str, url: str, default_timeout: int = CONNECTION_TIMEOUT_DEFAULT):
        # Temporarily unlock `_auth` for `super().__init__()`
        self._auth_locked = False
        super(BackendConnection, self).__init__(url, default_timeout=default_timeout)
        self._auth = None
        self._auth_locked = True

        self.id = id
        self.default_headers["User-Agent"] = "openeo-aggregator/{v}".format(
            v=openeo_aggregator.about.__version__,
        )
        # Mapping of aggregator provider id to backend's provider id
        self._oidc_provider_map: Dict[str, str] = {}

    def _get_auth(self) -> Union[None, OpenEoApiAuthBase]:
        return None if self._auth_locked else self._auth

    def _set_auth(self, auth: OpenEoApiAuthBase):
        if self._auth_locked:
            raise LockedAuthException("Setting auth while locked.")
        self._auth = auth

    auth = property(_get_auth, _set_auth)

    def set_oidc_provider_map(self, pid_map: Dict[str, str]):
        if len(self._oidc_provider_map) > 0 and self._oidc_provider_map != pid_map:
            _log.warning(
                f"Changing OIDC provider mapping in connection {self.id}"
                f" from {self._oidc_provider_map} to {pid_map}"
            )
        _log.info(f"Setting OIDC provider mapping for connection {self.id}: {pid_map}")
        self._oidc_provider_map = pid_map

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


_ConnectionsCache = collections.namedtuple("_ConnectionsCache", ["expiry", "connections"])


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """

    # TODO: move this caching ttl to config?
    _CONNECTION_CACHING_TTL = 5 * 60

    _TIMEOUT = 5

    # TODO: keep track of (recent) backend failures, e.g. to automatically blacklist a backend
    # TODO: synchronized backend connection caching/flushing across gunicorn workers, for better consistency?

    def __init__(self, backends: Dict[str, str]):
        if any(not re.match(r"^[a-z0-9]+$", bid) for bid in backends.keys()):
            raise ValueError(
                f"Backend ids should be alphanumeric only (no dots, dashes, ...) "
                f"to avoid collision issues when used as prefix. Got: {list(backends.keys())}"
            )
        # TODO: backend_urls as dict does not have explicit order, while this is important.
        self._backend_urls = backends
        self._connections_cache = _ConnectionsCache(expiry=0, connections=[])
        # TODO: API version management: just do single-version aggregation, or also handle version discovery?
        self.api_version = self._get_api_version()
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def _get_connections(self, skip_failure=False) -> Iterator[BackendConnection]:
        """Create new backend connections."""
        for (bid, url) in self._backend_urls.items():
            try:
                _log.info(f"Create backend {bid!r} connection to {url!r}")
                # TODO: also do a health check on the connection?
                yield BackendConnection(id=bid, url=url)
            except Exception as e:
                if skip_failure:
                    _log.warning(f"Failed to create backend {bid!r} connection to {url!r}: {e!r}")
                else:
                    raise

    def get_connections(self) -> List[BackendConnection]:
        """Get backend connections (re-created automatically if cache ttl expired)"""
        now = time.time()
        if now > self._connections_cache.expiry:
            _log.info(f"Connections cache miss: setting up new connections")
            self._connections_cache = _ConnectionsCache(
                expiry=now + self._CONNECTION_CACHING_TTL,
                connections=list(self._get_connections(skip_failure=True))
            )
            _log.info(
                f"Created {len(self._connections_cache.connections)} actual"
                f" of {len(self._backend_urls)} configured connections"
            )
        return self._connections_cache.connections

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self.get_connections())

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

    def map(self, callback: Callable[[BackendConnection], Any]) -> Iterator[Tuple[str, Any]]:
        """
        Query each backend connection with given callable and return results as iterator

        :param callback: function to apply to the connection
        """
        for con in self.get_connections():
            res = callback(con)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res

    def get_oidc_providers_per_backend(self) -> Dict[str, List[OidcProvider]]:
        return self._cache.get_or_call(key="oidc_providers_per_backend", callback=self._get_oidc_providers_per_backend)

    def _get_oidc_providers_per_backend(self) -> Dict[str, List[OidcProvider]]:
        # Collect provider info per backend
        providers_per_backend: Dict[str, List[OidcProvider]] = {}
        for con in self.get_connections():
            providers_per_backend[con.id] = []
            for provider_data in con.get("/credentials/oidc", expected_status=200).json()["providers"]:
                # Normalize issuer for sensible comparison operations.
                provider_data["issuer"] = provider_data["issuer"].rstrip("/").lower()
                providers_per_backend[con.id].append(OidcProvider.from_dict(provider_data))
        return providers_per_backend

    def build_oidc_handling(self, configured_providers: List[OidcProvider]) -> List[OidcProvider]:
        """
        Determine OIDC providers to use in aggregator (based on OIDC issuers supported by all backends)
        and set up provider id mapping in the backend connections

        :param configured_providers: OIDC providers dedicated/configured for the aggregator
        :return: list of actual OIDC providers to use (configured for aggregator and supported by all backends)
        """
        providers_per_backend = self.get_oidc_providers_per_backend()

        # Find OIDC issuers supported by each backend (intersection of issuer sets).
        issuers_per_backend = [
            set(p.issuer for p in providers)
            for providers in providers_per_backend.values()
        ]
        intersection: Set[str] = functools.reduce((lambda x, y: x.intersection(y)), issuers_per_backend)
        _log.info(f"OIDC provider intersection: {intersection}")
        if len(intersection) == 0:
            _log.warning(f"Emtpy OIDC provider intersection. Issuers per backend: {issuers_per_backend}")

        # Take configured providers for common issuers.
        agg_providers = [p for p in configured_providers if p.issuer.rstrip("/").lower() in intersection]
        _log.info(f"Actual aggregator providers: {agg_providers}")

        # Set up provider id mapping (aggregator pid to original backend pid) for the connections
        for con in self.get_connections():
            backend_providers = providers_per_backend[con.id]
            pid_map = {}
            for agg_provider in agg_providers:
                agg_issuer = agg_provider.issuer.rstrip("/").lower()
                orig_pid = next(bp.id for bp in backend_providers if bp.issuer == agg_issuer)
                pid_map[agg_provider.id] = orig_pid
            con.set_oidc_provider_map(pid_map)

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
