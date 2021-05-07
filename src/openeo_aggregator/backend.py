import logging
from collections import namedtuple
from typing import List, Dict, Any, Iterator, Callable, Tuple

import openeo
from openeo import Connection
from openeo.rest import OpenEoApiError
from openeo_aggregator.utils import TtlCache
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)

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

    def __iter__(self) -> Iterator[BackendConnection]:
        return iter(self.connections)

    def map(self, callback: Callable[[Connection], Any]) -> Iterator[Tuple[str, Any]]:
        """
        Query each backend connection with given callable and return results as iterator

        :param callback: function to apply to the connection
        """
        for con in self.connections:
            res = callback(con.connection)
            # TODO: customizable exception handling: skip, warn, re-raise?
            yield con.id, res


class FederationCollectionCatalog(AbstractCollectionCatalog):

    def __init__(self, backends: MultiBackendConnection):
        self.backends = backends
        self._cache = TtlCache(default_ttl=60)

    def get_all_metadata(self) -> List[dict]:
        return self._cache.get_or_call(
            key=("all",),
            callback=self._get_all_metadata
        )

    def _get_all_metadata(self) -> List[dict]:
        all_collections = []
        for backend in self.backends:
            try:
                all_collections.extend(backend.connection.list_collections())
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get collections from {backend.id}", exc_info=True)
        return all_collections

    def get_collection_metadata(self, collection_id: str) -> dict:
        return self._cache.get_or_call(
            key=("collection", collection_id),
            callback=self._get_collection_metadata
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


class FederationBackendImplementation(OpenEoBackendImplementation):

    def __init__(self):
        # TODO: move this to some kind of config?
        backends = MultiBackendConnection({
            "vito": "https://openeo.vito.be/openeo/1.0",
            "eodc": "https://openeo.eodc.eu/v1.0",
        })
        super().__init__(
            secondary_services=None,
            catalog=FederationCollectionCatalog(backends=backends),
            batch_jobs=None,
            user_defined_processes=None
        )


def get_openeo_backend_implementation() -> FederationBackendImplementation:
    return FederationBackendImplementation()
