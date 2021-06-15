import logging
from typing import List, Dict, Union

import flask

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT, CACHE_TTL_DEFAULT
from openeo_aggregator.connection import MultiBackendConnection
from openeo_aggregator.utils import TtlCache
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters, Processing, \
    OidcProvider
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)


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
        for con in self.backends:
            try:
                backend_collections = con.list_collections()
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get collections from {con.id}", exc_info=True)
            else:
                for collection_metadata in backend_collections:
                    cid = collection_metadata["id"]
                    if cid in all_collections:
                        duplicates.add(cid)
                    collection_metadata[self.METADATA_KEY] = {"backend": {"id": con.id, "url": con.root_url}}
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
        for con in self.backends:
            try:
                return con.describe_collection(name=collection_id)
            except OpenEoApiError as e:
                if e.code == "CollectionNotFound":
                    continue
                _log.warning(f"Unexpected error on lookup of collection {collection_id} at {con.id}", exc_info=True)
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
        for con in self.backends:
            try:
                processes_per_backend[con.id] = {p["id"]: p for p in con.list_processes()}
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get processes from {con.id}", exc_info=True)

        # TODO: not only check process name, but also parameters and return type?
        # TODO: return union of processes instead of intersection?
        intersection = None
        for bid, backend_processes in processes_per_backend.items():
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
        con = self.backends.get_connection(backend_id=backend_id)
        request_pg = {"process": {"process_graph": process_graph}}
        with con.authenticated_from_request(flask.request):
            backend_response = con.post(path="/result", json=request_pg, stream=True)

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
        return self._backends.get_oidc_providers()
