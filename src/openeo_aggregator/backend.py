import functools
import logging
from collections import namedtuple
from typing import List, Dict, Any, Iterator, Callable, Tuple, Union

import flask

import openeo
from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
import openeo_aggregator.about
from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT
from openeo_aggregator.utils import TtlCache
from openeo_driver.backend import (
    OpenEoBackendImplementation,
    AbstractCollectionCatalog,
    LoadParameters,
    Processing,
    OidcProvider,
)
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException, AuthenticationRequiredException, \
    AuthenticationSchemeInvalidException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)

CACHE_TTL_DEFAULT = 5 * 60

BackendConnection = namedtuple("BackendConnection", ["id", "url", "connection"])

OidcProviderData = namedtuple("OidcProviderData", ["provider_list", "provider_id_map"])


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """

    _TIMEOUT = 5

    def __init__(self, backends: Dict[str, str]):
        self._backends = backends
        self.connections = []
        for (bid, url) in backends.items():
            _log.info(f"Setting up backend {bid!r} Connection: {url!r}")
            connection = openeo.connect(url, default_timeout=self._TIMEOUT)
            connection.default_headers["User-Agent"] = "openeo-aggregator/{v}".format(
                v=openeo_aggregator.about.__version__,
            )
            self.connections.append(BackendConnection(bid, url, connection))
        # TODO: API version management: just do single-version aggregation, or also handle version discovery?
        self.api_version = self._get_api_version()
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self.connections)

    def first(self) -> BackendConnection:
        """Get first backend in the list"""
        # TODO: rename this to main_backend (if it makes sense to have a general main backend)?
        return self.connections[0]

    def get_connection(self, backend_id: str) -> BackendConnection:
        return next(c for c in self if c.id == backend_id)

    def _get_api_version(self) -> ComparableVersion:
        # TODO: ignore patch level of API versions?
        versions = set(v for (i, v) in self.map(lambda c: c.capabilities().api_version()))
        if len(versions) != 1:
            raise OpenEOApiException(f"Only single version is supported, but found: {versions}")
        return ComparableVersion(versions.pop())

    def map(self, callback: Callable[[Connection], Any]) -> Iterator[Tuple[str, Any]]:
        """
        Query each backend connection with given callable and return results as iterator

        :param callback: function to apply to the connection
        """
        for con in self.connections:
            res = callback(con.connection)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res

    def get_oidc_data(self) -> OidcProviderData:
        return self._cache.get_or_call(key="oidc_data", callback=self._get_oidc_data)

    def _get_oidc_data(self) -> OidcProviderData:
        """Build list of common OIDC providers and OIDC provider id mapping"""
        # Collect provider info per backend
        providers_per_backend = {}
        for backend in self:
            providers = backend.connection.get("/credentials/oidc", expected_status=200).json()["providers"]
            for p in providers:
                # Normalize issuer a bit to have useful intersection later.
                p["issuer"] = p["issuer"].rstrip("/")
            providers_per_backend[backend.id] = providers

        # Calculate intersection (based on issuer URL)
        issuers_per_backend = [
            set(p["issuer"] for p in providers)
            for providers in providers_per_backend.values()
        ]
        intersection = functools.reduce((lambda x, y: x.intersection(y)), issuers_per_backend)
        if len(intersection) == 0:
            _log.warning(f"Emtpy OIDC intersection. Issuers per backend: {issuers_per_backend}")

        providers = []
        pid_map = {}
        for provider_data in providers_per_backend[self.first().id]:
            issuer = provider_data["issuer"]
            if issuer in intersection:
                pid = provider_data["id"]
                providers.append(OidcProvider(
                    pid,
                    issuer=issuer,
                    title=provider_data["title"],
                    scopes=provider_data.get("scopes", ["openid"]),
                    default_clients=provider_data.get("default_clients"),
                ))

                # Mapping of backend id to original provider id of this issuer
                pid_map[pid] = {
                    bid: next(p["id"] for p in ps if p["issuer"] == issuer)
                    for bid, ps in providers_per_backend.items()
                }

        return OidcProviderData(provider_list=providers, provider_id_map=pid_map)

    def build_authorization_header(self, backend: BackendConnection, request: flask.Request = None) -> dict:
        """Extract authorization header from request and (optionally) transform for given backend """
        request = request or flask.request
        if "Authorization" not in request.headers:
            raise AuthenticationRequiredException
        auth = request.headers["Authorization"]
        if auth.startswith("Bearer basic//"):
            pass
        elif auth.startswith("Bearer oidc/"):
            _, pid, token = auth.split("/")
            backend_pid = self.get_oidc_data().provider_id_map[pid][backend.id]
            auth = f"Bearer oidc/{backend_pid}/{token}"
        else:
            raise AuthenticationSchemeInvalidException
        return {"Authorization": auth}


class AggregatorCollectionCatalog(AbstractCollectionCatalog):
    METADATA_KEY = "_aggregator"

    def __init__(self, backends: MultiBackendConnection):
        self.backends = backends
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def get_all_metadata(self) -> List[dict]:
        return self._cache.get_or_call(
            key=("all",),
            callback=self._get_all_metadata,
        )

    def _get_all_metadata(self) -> List[dict]:
        all_collections = {}
        duplicates = set([])
        for backend in self.backends:
            try:
                backend_collections = backend.connection.list_collections()
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get collections from {backend.id}", exc_info=True)
            else:
                for collection_metadata in backend_collections:
                    cid = collection_metadata["id"]
                    if cid in all_collections:
                        duplicates.add(cid)
                    collection_metadata[self.METADATA_KEY] = {"backend": {"id": backend.id, "url": backend.url}}
                    all_collections[cid] = collection_metadata
        for cid in duplicates:
            # TODO resolve duplication issue in more forgiving way?
            _log.warning(f"Not exposing duplicated collection id {cid}")
            del all_collections[cid]
        return list(all_collections.values())

    def get_collection_metadata(self, collection_id: str) -> dict:
        return self._cache.get_or_call(
            key=("collection", collection_id),
            callback=lambda: self._get_collection_metadata(collection_id),
        )

    def _get_collection_metadata(self, collection_id: str) -> dict:
        for backend in self.backends:
            try:
                return backend.connection.describe_collection(name=collection_id)
            except OpenEoApiError as e:
                if e.code == "CollectionNotFound":
                    continue
                _log.warning(f"Unexpected error on lookup of collection {collection_id} at {backend.id}", exc_info=True)
        raise CollectionNotFoundException(collection_id)

    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> DriverDataCube:
        raise RuntimeError("openeo-aggregator does not implement concrete collection loading")


class AggregatorProcessing(Processing):
    def __init__(
            self,
            backends: MultiBackendConnection,
            catalog: AggregatorCollectionCatalog,
            stream_chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT
    ):
        self.backends = backends
        # TODO Cache per backend results instead of output?
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)
        self._catalog = catalog
        self._stream_chunk_size = stream_chunk_size

    def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
        if api_version != self.backends.api_version:
            raise OpenEOApiException(
                message=f"Requested API version {api_version} != expected {self.backends.api_version}"
            )
        return self._cache.get_or_call(key=str(api_version), callback=self._get_process_registry)

    def _get_process_registry(self) -> ProcessRegistry:
        processes_per_backend = {}
        for backend in self.backends:
            try:
                processes_per_backend[backend.id] = {p["id"]: p for p in backend.connection.list_processes()}
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get processes from {backend.id}", exc_info=True)

        # TODO: not only check process name, but also parameters and return type?
        # TODO: return union of processes instead of intersection?
        intersection = None
        for backend, backend_processes in processes_per_backend.items():
            if intersection is None:
                intersection = backend_processes
            else:
                intersection = {k: v for (k, v) in intersection.items() if k in backend_processes}

        process_registry = ProcessRegistry()
        for pid, spec in intersection.items():
            process_registry.add_spec(spec=spec)

        return process_registry

    def _get_collection_map(self) -> Dict[str, str]:
        """Get mapping of collection id to backend it's hosted on."""
        return {
            collection["id"]: collection[AggregatorCollectionCatalog.METADATA_KEY]["backend"]["id"]
            for collection in self._catalog.get_all_metadata()
        }

    def evaluate(self, process_graph: dict, env: EvalEnv = None):
        """Evaluate given process graph (flat dict format)."""

        # Check used collections to determine which backend to use
        collections = set(
            n["arguments"]["id"]
            for n in process_graph.values()
            if n["process_id"] == "load_collection"
        )
        collection_map = self._get_collection_map()
        backends = set(collection_map[cid] for cid in collections)

        if len(backends) == 1:
            backend_id = backends.pop()
        elif len(backends) == 0:
            backend_id = self.backends.first().id
        else:
            raise OpenEOApiException(
                message=f"Collections across multiple backends: {collections}."
            )

        # Send process graph to backend
        backend = self.backends.get_connection(backend_id=backend_id)
        request_pg = {"process": {"process_graph": process_graph}}
        headers = self.backends.build_authorization_header(backend=backend)
        backend_response = backend.connection.post(path="/result", json=request_pg, headers=headers, stream=True)

        # Convert `requests.Response` from backend to `flask.Response` for client
        headers = [(k, v) for (k, v) in backend_response.headers.items() if k.lower() in ["content-type"]]
        return flask.Response(
            # Streaming response through `iter_content` generator (https://flask.palletsprojects.com/en/2.0.x/patterns/streaming/)
            response=backend_response.iter_content(chunk_size=self._stream_chunk_size),
            status=backend_response.status_code,
            headers=headers,
        )


class AggregatorBackendImplementation(OpenEoBackendImplementation):
    def __init__(self, backends: MultiBackendConnection, config: AggregatorConfig):
        self._backends = backends
        catalog = AggregatorCollectionCatalog(backends=backends)
        processing = AggregatorProcessing(
            backends=backends, catalog=catalog,
            stream_chunk_size=config.streaming_chunk_size,
        )
        super().__init__(
            catalog=catalog,
            processing=processing,
            secondary_services=None,
            batch_jobs=None,
            user_defined_processes=None,
        )

    def oidc_providers(self) -> List[OidcProvider]:
        return self._backends.get_oidc_data().provider_list
