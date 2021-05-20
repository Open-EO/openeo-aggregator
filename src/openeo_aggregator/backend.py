import logging
from collections import namedtuple
from typing import List, Dict, Any, Iterator, Callable, Tuple, Union

import openeo
from openeo import Connection
from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
from openeo_aggregator.utils import TtlCache
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters, Processing
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)

CACHE_TTL_DEFAULT = 5 * 60

BackendConnection = namedtuple("BackendConnection", ["id", "url", "connection"])


class MultiBackendConnection:
    """
    Collection of multiple connections to different backends
    """
    _TIMEOUT = 5

    def __init__(self, backends: Dict[str, str]):
        self._backends = backends
        self.connections = [
            BackendConnection(bid, url, openeo.connect(url, default_timeout=self._TIMEOUT))
            for (bid, url) in backends.items()
        ]
        self.api_version = self._get_api_version()

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self.connections)

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


class AggregatorCollectionCatalog(AbstractCollectionCatalog):

    def __init__(self, backends: MultiBackendConnection):
        self.backends = backends
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def get_all_metadata(self) -> List[dict]:
        return self._cache.get_or_call(
            key=("all",),
            callback=self._get_all_metadata
        )

    def _get_all_metadata(self) -> List[dict]:
        all_collections = {}
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
                        message = f"Duplicate collection id {cid}"
                        # TODO resolve duplication issue in more forgiving way?
                        _log.error(message)
                        raise OpenEOApiException(message=message)
                    all_collections[cid] = collection_metadata
        return list(all_collections.values())

    def get_collection_metadata(self, collection_id: str) -> dict:
        return self._cache.get_or_call(
            key=("collection", collection_id),
            callback=lambda: self._get_collection_metadata(collection_id)
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

    def __init__(self, backends: MultiBackendConnection):
        self.backends = backends
        # TODO Cache per backend results instead of output?
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
        assert api_version == self.backends.api_version
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


class AggregatorBackendImplementation(OpenEoBackendImplementation):

    def __init__(self):
        backends = MultiBackendConnection({
            # TODO: move this to some kind of config?
            # TODO: API version management: just do single-version aggregation, or also handle version discovery?
            "vito": "https://openeo.vito.be/openeo/1.0",
            "eodc": "https://openeo.eodc.eu/v1.0",
        })
        super().__init__(
            catalog=AggregatorCollectionCatalog(backends=backends),
            processing=AggregatorProcessing(backends=backends),
            secondary_services=None,
            batch_jobs=None,
            user_defined_processes=None
        )
