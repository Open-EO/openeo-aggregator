import collections
import concurrent.futures
import contextlib
import dataclasses
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import flask
import requests
from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo.rest.auth.auth import BearerAuth, OpenEoApiAuthBase
from openeo.rest.connection import RestApiConnection
from openeo.util import TimingLogger
from openeo_driver.backend import OidcProvider
from openeo_driver.errors import (
    AuthenticationRequiredException,
    AuthenticationSchemeInvalidException,
    InternalException,
    OpenEOApiException,
)
from openeo_driver.users import User

import openeo_aggregator.about
from openeo_aggregator.caching import Memoizer, NullMemoizer, memoizer_from_config
from openeo_aggregator.config import (
    CONNECTION_TIMEOUT_DEFAULT,
    CONNECTION_TIMEOUT_INIT,
    STREAM_CHUNK_SIZE_DEFAULT,
    get_backend_config,
)
from openeo_aggregator.utils import _UNSET, Clock, EventHandler

_log = logging.getLogger(__name__)


# Type annotation aliases

BackendId = str


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
        *,
        configured_oidc_providers: Optional[List[OidcProvider]] = None,
        default_timeout: int = CONNECTION_TIMEOUT_DEFAULT,
        init_timeout: int = CONNECTION_TIMEOUT_INIT,
    ):
        # Temporarily unlock `_auth` for `super().__init__()`
        self._auth_locked = False
        super(BackendConnection, self).__init__(
            url, default_timeout=init_timeout, slow_response_threshold=1, auto_validate=False
        )
        self._auth = None
        self._auth_locked = True

        self.id = id
        self.default_headers["User-Agent"] = "openeo-aggregator/{v}".format(
            v=openeo_aggregator.about.__version__,
        )
        # Mapping of aggregator provider id to backend's provider id
        if configured_oidc_providers is None:
            configured_oidc_providers = get_backend_config().oidc_providers
        self._oidc_provider_map: Dict[str, str] = self._build_oidc_provider_map(configured_oidc_providers)

        self.default_timeout = default_timeout

    def __repr__(self):
        return f"<{type(self).__name__} {self.id}: {self._root_url}>"

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
        _log.debug(f"_build_oidc_provider_map {pid_map=}")
        return pid_map

    def get_oidc_provider_map(self) -> Dict[str, str]:
        return self._oidc_provider_map

    def extract_bearer(self, request: flask.Request) -> str:
        """
        Extract authorization header from flask request
        and (optionally) transform for current backend.
        """
        if "Authorization" not in request.headers:
            raise AuthenticationRequiredException
        auth = request.headers["Authorization"]

        if auth.startswith("Bearer basic//"):
            return auth.partition("Bearer ")[2]
        elif auth.startswith("Bearer oidc/"):
            _, pid, token = auth.split("/")
            try:
                backend_pid = self._oidc_provider_map[pid]
            except KeyError:
                _log.error(f"Back-end {self} lacks OIDC provider support: {pid!r} not in {self._oidc_provider_map}.")
                raise OpenEOApiException(
                    code="OidcSupportError", message=f"Back-end {self.id!r} does not support OIDC provider {pid!r}."
                )
            return f"oidc/{backend_pid}/{token}"
        else:
            raise AuthenticationSchemeInvalidException

    @contextlib.contextmanager
    def authenticated_from_request(
        self, request: flask.Request, user: Optional[User] = None
    ) -> Iterator["BackendConnection"]:
        """
        Context manager to temporarily authenticate upstream connection based on current incoming flask request.
        """
        self._auth_locked = False
        self.auth = BearerAuth(bearer=self.extract_bearer(request=request))
        # TODO store and use `user` object?
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
        try:
            if default_timeout is not _UNSET:
                self.default_timeout = default_timeout
            if default_headers is not _UNSET:
                self.default_headers = default_headers
            yield self
        finally:
            self.default_timeout = orig_default_timeout
            self.default_headers = orig_default_headers

    def invalidate(self):
        """Destroy connection to avoid accidental usage."""

        def request(*args, **kwargs):
            raise InvalidatedConnection

        self.request = request


_ConnectionsCache = collections.namedtuple("_ConnectionsCache", ["expiry", "connections"])


@dataclasses.dataclass(frozen=True)
class ParallelResponse:
    """Set of responses and failures info for a parallelized request."""

    successes: Dict[BackendId, dict]
    failures: Dict[BackendId, Exception]


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """

    # TODO: API version management: just do single/fixed-version federation, or also handle version discovery?
    # TODO: keep track of (recent) backend failures, e.g. to automatically blacklist a backend
    # TODO: synchronized backend connection caching/flushing across gunicorn workers, for better consistency?

    _TIMEOUT = 5

    def __init__(
        self,
        backends: Dict[str, str],
        configured_oidc_providers: List[OidcProvider],
        memoizer: Memoizer = None,
        connections_cache_ttl: float = 5 * 60.0,
    ):
        if any(not re.match(r"^[a-z0-9]+$", bid) for bid in backends.keys()):
            raise ValueError(
                f"Backend ids should be alphanumeric only (no dots, dashes, ...) "
                f"to avoid collision issues when used as prefix. Got: {list(backends.keys())}"
            )
        # TODO: backend_urls as dict does not have explicit order, while this is important.
        _log.info(f"Creating MultiBackendConnection with {backends=!r}")
        self._backend_urls = backends
        self._configured_oidc_providers = configured_oidc_providers

        # General (metadata/status) caching
        self._memoizer: Memoizer = memoizer or NullMemoizer()

        # Caching of connection objects
        self._connections_cache = _ConnectionsCache(expiry=0, connections=[])
        self._connections_cache_ttl = connections_cache_ttl
        # Event handler for when there is a change in the set of working back-end ids.
        self.on_connections_change = EventHandler("connections_change")
        self.on_connections_change.add(self._memoizer.invalidate)

    @staticmethod
    def from_config() -> "MultiBackendConnection":
        backend_config = get_backend_config()
        return MultiBackendConnection(
            backends=backend_config.aggregator_backends,
            configured_oidc_providers=backend_config.oidc_providers,
            memoizer=memoizer_from_config(namespace="mbcon"),
            connections_cache_ttl=backend_config.connections_cache_ttl,
        )

    def _get_connections(self, skip_failures=False) -> Iterator[BackendConnection]:
        """Create new backend connections."""
        for bid, url in self._backend_urls.items():
            try:
                _log.info(f"Create backend {bid!r} connection to {url!r}")
                # TODO: Creating connection usually involves version discovery and request of capability doc.
                #       Additional health check necessary?
                yield BackendConnection(id=bid, url=url, configured_oidc_providers=self._configured_oidc_providers)
            except Exception as e:
                _log.warning(f"Failed to create backend {bid!r} connection to {url!r}: {e!r}", exc_info=True)
                if not skip_failures:
                    raise

    def get_connections(self) -> List[BackendConnection]:
        """Get backend connections (re-created automatically if cache ttl expired)"""
        now = Clock.time()
        if now > self._connections_cache.expiry:
            _log.debug(f"Connections cache expired ({now:.2f}>{self._connections_cache.expiry:.2f})")
            orig_bids = [c.id for c in self._connections_cache.connections]
            for con in self._connections_cache.connections:
                con.invalidate()
            self._connections_cache = _ConnectionsCache(
                expiry=now + self._connections_cache_ttl, connections=list(self._get_connections(skip_failures=True))
            )
            new_bids = [c.id for c in self._connections_cache.connections]
            _log.debug(
                f"Created {len(self._connections_cache.connections)} actual"
                f" of {len(self._backend_urls)} configured connections"
                f" (TTL {self._connections_cache_ttl}s)"
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

    def _get_api_versions(self) -> List[str]:
        return list(set(c.capabilities().api_version() for c in self.get_connections()))

    def get_api_versions(self) -> Set[ComparableVersion]:
        """Get set of API versions reported by backends"""
        versions = self._memoizer.get_or_call(key="api_versions", callback=self._get_api_versions)
        versions = set(ComparableVersion(v) for v in versions)
        return versions

    @property
    def api_version_minimum(self) -> ComparableVersion:
        """Get the lowest API version of all back-ends"""
        return min(self.get_api_versions())

    @property
    def api_version_maximum(self) -> ComparableVersion:
        """Get the highest API version of all back-ends"""
        return max(self.get_api_versions())

    def map(self, callback: Callable[[BackendConnection], Any]) -> Iterator[Tuple[str, Any]]:
        """
        Query each backend connection with given callable and return results as iterator

        :param callback: function to apply to the connection
        """
        for con in self.get_connections():
            res = callback(con)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res

    def request_parallel(
        self,
        path: str,
        *,
        method: str = "GET",
        parse_json: bool = True,
        authenticated_from_request: Optional[flask.Request] = None,
        expected_status: Union[int, Iterable[int], None] = None,
        request_timeout: float = 5,
        overall_timeout: float = 8,
        max_workers=5,
    ) -> ParallelResponse:
        """
        Request a given (relative) url on each backend in parallel
        :param path: relative (openEO) path to request
        :return:
        """

        def do_request(
            root_url: str,
            path: str,
            *,
            method: str = "GET",
            headers: Optional[dict] = None,
            auth: Optional[str] = None,
        ) -> Union[dict, bytes]:
            """Isolated request, to behanled by future."""
            with TimingLogger(title=f"request_parallel {method} {path} on {root_url}", logger=_log):
                con = RestApiConnection(root_url=root_url)
                resp = con.request(
                    method=method,
                    path=path,
                    headers=headers,
                    auth=auth,
                    timeout=request_timeout,
                    expected_status=expected_status,
                )
            if parse_json:
                return resp.json()
            else:
                return resp.content

        connections: List[BackendConnection] = self.get_connections()
        max_workers = min(max_workers, len(connections))

        with TimingLogger(
            title=f"request_parallel {method} {path} on {len(connections)} backends with thread pool {max_workers=}",
            logger=_log,
        ), concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all futures (one for each backend connection)
            futures: List[Tuple[BackendId, concurrent.futures.Future]] = []
            for con in connections:
                if authenticated_from_request:
                    auth = BearerAuth(bearer=con.extract_bearer(request=authenticated_from_request))
                else:
                    auth = None
                future = executor.submit(
                    do_request,
                    root_url=con.root_url,
                    path=path,
                    method=method,
                    headers=con.default_headers,
                    auth=auth,
                )
                futures.append((con.id, future))

            # Give futures some time to finish
            concurrent.futures.wait([f for (_, f) in futures], timeout=overall_timeout)

            # Collect results.
            successes = {}
            failures = {}
            for backend_id, future in futures:
                try:
                    successes[backend_id] = future.result(timeout=0)
                except Exception as e:
                    failures[backend_id] = e

            return ParallelResponse(successes=successes, failures=failures)


def streaming_flask_response(
    backend_response: requests.Response, chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT
) -> flask.Response:
    """
    Convert a `requests.Response` coming from a backend
    to a (streaming) `flask.Response` to send to the client

    :param backend_response: `requests.Response` object (possibly created with "stream" option enabled)
    :param chunk_size: chunk size to use for streaming
    """
    headers = [(k, v) for (k, v) in backend_response.headers.items() if k.lower() in ["content-type"]]
    return flask.Response(
        # Streaming response through `iter_content` generator (https://flask.palletsprojects.com/en/2.0.x/patterns/streaming/)
        response=backend_response.iter_content(chunk_size=chunk_size),
        status=backend_response.status_code,
        headers=headers,
    )
