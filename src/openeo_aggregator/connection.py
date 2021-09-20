import contextlib
import functools
import logging
import re
from typing import List, Dict, Any, Iterator, Callable, Tuple, Set, Union

import flask

import openeo_aggregator.about
from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo.rest.auth.auth import BearerAuth, OpenEoApiAuthBase
from openeo_aggregator.config import CACHE_TTL_DEFAULT, CONNECTION_TIMEOUT_DEFAULT
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


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """

    _TIMEOUT = 5

    def __init__(self, backends: Dict[str, str]):
        if any(not re.match(r"^[a-z0-9]+$", bid) for bid in backends.keys()):
            raise ValueError(
                f"Backend ids should be alphanumeric only (no dots, dashes, ...) "
                f"to avoid collision issues when used as prefix. Got: {list(backends.keys())}"
            )
        self._backend_urls = backends
        self._connections: List[BackendConnection] = []
        for (bid, url) in self._backend_urls.items():
            _log.info(f"Setting up backend {bid!r} Connection: {url!r}")
            self._connections.append(BackendConnection(id=bid, url=url))
        # TODO: API version management: just do single-version aggregation, or also handle version discovery?
        self.api_version = self._get_api_version()
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

        _log.info("Prime OIDC provider data")
        self.get_oidc_providers()

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self._connections)

    def first(self) -> BackendConnection:
        """Get first backend in the list"""
        # TODO: rename this to main_backend (if it makes sense to have a general main backend)?
        return self._connections[0]

    def get_connection(self, backend_id: str) -> BackendConnection:
        for con in self:
            if con.id == backend_id:
                return con
        raise OpenEOApiException(f"No backend with id {backend_id!r}")

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
        for con in self._connections:
            res = callback(con)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res

    def get_oidc_providers(self) -> List[OidcProvider]:
        return self._cache.get_or_call(key="oidc_data", callback=self._build_oidc_data)

    def _build_oidc_data(self) -> List[OidcProvider]:
        """
        Build list of common OIDC providers to advertise as aggregator OIDC provider
        and set up the provider mapping in the connections
        """
        # Collect provider info per backend
        providers_per_backend: Dict[str, List[OidcProvider]] = {}
        for con in self._connections:
            providers_per_backend[con.id] = []
            for provider_data in con.get("/credentials/oidc", expected_status=200).json()["providers"]:
                # Normalize issuer a bit to have useful intersection later.
                provider_data["issuer"] = provider_data["issuer"].rstrip("/")
                providers_per_backend[con.id].append(OidcProvider.from_dict(provider_data))

        # Calculate intersection (based on issuer URL)
        issuers_per_backend = [
            set(p.issuer for p in providers)
            for providers in providers_per_backend.values()
        ]
        intersection: Set[str] = functools.reduce((lambda x, y: x.intersection(y)), issuers_per_backend)
        _log.info(f"OIDC provider intersection: {intersection}")
        if len(intersection) == 0:
            _log.warning(f"Emtpy OIDC provider intersection. Issuers per backend: {issuers_per_backend}")
        agg_providers = [
            p for p in providers_per_backend[self.first().id]
            if p.issuer in intersection
        ]

        # Build and register mapping of aggregator provider id to backend provider id.
        for con in self._connections:
            backend_providers = providers_per_backend[con.id]
            pid_map = {}
            for agg_provider in agg_providers:
                pid_map[agg_provider.id] = next(bp.id for bp in backend_providers if bp.issuer == agg_provider.issuer)
            con.set_oidc_provider_map(pid_map)

        return agg_providers
