import logging
from typing import List

import openeo
from openeo.rest import OpenEoApiError
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.utils import EvalEnv


# TODO: move this to some kind of config?
# TODO: encapsulate in some kind of multi-backend connection object that takes care of looping, try-excepting, ...
backends = [
    "https://openeo.vito.be/openeo/1.0",
    "https://openeo.eodc.eu/v1.0",
]

_log = logging.getLogger(__name__)


class FederationCollectionCatalog(AbstractCollectionCatalog):

    def __init__(self, backends: List[str]):
        self.backends = backends

    def get_all_metadata(self) -> List[dict]:
        all_collections = []
        for backend in self.backends:
            try:
                # TODO: add field to reference original backend?
                all_collections.extend(openeo.connect(backend, default_timeout=5).list_collections())
            except Exception:
                _log.warning(f"Failed to get collections from {backend}", exc_info=True)
        return all_collections

    def get_collection_metadata(self, collection_id: str) -> dict:
        for backend in self.backends:
            try:
                return openeo.connect(backend, default_timeout=5).describe_collection(name=collection_id)
            except OpenEoApiError as e:
                if e.code == "CollectionNotFound":
                    continue
                _log.warning(f"Unexpected error on lookup of collection {collection_id} at {backend}", exc_info=True)
        raise CollectionNotFoundException(collection_id)

    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> DriverDataCube:
        raise NotImplementedError


class FederationBackendImplementation(OpenEoBackendImplementation):

    def __init__(self):
        super().__init__(
            secondary_services=None,
            catalog=FederationCollectionCatalog(backends=backends),
            batch_jobs=None,
            user_defined_processes=None
        )


def get_openeo_backend_implementation() -> FederationBackendImplementation:
    return FederationBackendImplementation()
