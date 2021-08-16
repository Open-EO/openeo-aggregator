import contextlib
import logging
from typing import List, Dict, Union, Tuple, Optional

import flask

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError
from openeo.util import dict_no_none
from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT, CACHE_TTL_DEFAULT, \
    CONNECTION_TIMEOUT_RESULT
from openeo_aggregator.connection import MultiBackendConnection, BackendConnection
from openeo_aggregator.utils import TtlCache
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters, Processing, \
    OidcProvider, BatchJobs, BatchJobMetadata
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException, ProcessGraphMissingException, \
    JobNotFoundException, JobNotFinishedException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.users import User
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

    def get_backend_for_process_graph(self, process_graph: dict) -> str:
        """
        Get backend capable of executing given process graph (based on used collections)
        """
        # TODO: also check used processes?
        try:
            collections = set(
                n["arguments"]["id"]
                for n in process_graph.values()
                if n["process_id"] == "load_collection"
            )
        except Exception:
            raise ProcessGraphMissingException()

        collection_map = self._get_collection_map()
        try:
            backends = set(collection_map[cid] for cid in collections)
        except KeyError as e:
            raise CollectionNotFoundException(collection_id=e.args[0])

        if len(backends) == 1:
            backend_id = backends.pop()
        elif len(backends) == 0:
            backend_id = self.backends.first().id
        else:
            raise OpenEOApiException(
                message=f"Collections across multiple backends: {collections}."
            )

        return backend_id

    def evaluate(self, process_graph: dict, env: EvalEnv = None):
        """Evaluate given process graph (flat dict format)."""

        backend_id = self.get_backend_for_process_graph(process_graph)

        # Send process graph to backend
        con = self.backends.get_connection(backend_id=backend_id)
        request_pg = {"process": {"process_graph": process_graph}}
        with con.authenticated_from_request(flask.request):
            backend_response = con.post(
                path="/result", json=request_pg,
                stream=True, timeout=CONNECTION_TIMEOUT_RESULT
            )

        # Convert `requests.Response` from backend to `flask.Response` for client
        headers = [(k, v) for (k, v) in backend_response.headers.items() if k.lower() in ["content-type"]]
        return flask.Response(
            # Streaming response through `iter_content` generator (https://flask.palletsprojects.com/en/2.0.x/patterns/streaming/)
            response=backend_response.iter_content(chunk_size=self._stream_chunk_size),
            status=backend_response.status_code,
            headers=headers,
        )


class AggregatorBatchJobs(BatchJobs):
    def __init__(self, backends: MultiBackendConnection, processing: AggregatorProcessing):
        super(AggregatorBatchJobs, self).__init__()
        self.backends = backends
        self.processing = processing

    def _get_aggregator_job_id(self, backend_job_id: str, backend_id: str) -> str:
        """Construct aggregator job id from given backend job id and backend id"""
        return f"{backend_id}-{backend_job_id}"

    def _parse_aggregator_job_id(self, aggregator_job_id: str) -> Tuple[str, str]:
        """Given aggregator job id: extract backend job id and backend id"""
        for prefix in [f"{con.id}-" for con in self.backends]:
            if aggregator_job_id.startswith(prefix):
                backend_id, backend_job_id = aggregator_job_id.split("-", maxsplit=1)
                return backend_job_id, backend_id
        raise JobNotFoundException(job_id=aggregator_job_id)

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        jobs = []
        for con in self.backends:
            with con.authenticated_from_request(request=flask.request):
                for job in con.list_jobs():
                    job["id"] = self._get_aggregator_job_id(backend_job_id=job["id"], backend_id=con.id)
                    jobs.append(BatchJobMetadata.from_dict(job))
        return jobs

    def create_job(
            self, user_id: str, process: dict, api_version: str,
            metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        try:
            process_graph = process["process_graph"]
        except (KeyError, TypeError) as e:
            raise ProcessGraphMissingException()
        backend_id = self.processing.get_backend_for_process_graph(process_graph=process_graph)
        con = self.backends.get_connection(backend_id)
        with con.authenticated_from_request(request=flask.request):
            try:
                job = con.create_job(
                    process_graph=process_graph,
                    title=metadata.get("title"), description=metadata.get("description"),
                    plan=metadata.get("plan"), budget=metadata.get("budget")
                )
            except OpenEoApiError as e:
                if e.code == "ProcessGraphMissing":
                    raise ProcessGraphMissingException()
                raise
        return BatchJobMetadata(
            id=self._get_aggregator_job_id(backend_job_id=job.job_id, backend_id=backend_id),
            status="dummy", created="dummy", process="dummy"
        )

    def _get_connection_and_backend_job_id(self, aggregator_job_id: str) -> Tuple[BackendConnection, str]:
        backend_job_id, backend_id = self._parse_aggregator_job_id(aggregator_job_id=aggregator_job_id)
        con = self.backends.get_connection(backend_id)
        return con, backend_job_id

    @contextlib.contextmanager
    def _translate_job_errors(self, job_id):
        """Context manager to translate job related errors, where necessary"""
        try:
            yield
        except OpenEoApiError as e:
            if e.code == JobNotFoundException.code:
                raise JobNotFoundException(job_id=job_id, )
            elif e.code == JobNotFinishedException.code:
                raise JobNotFinishedException(message=e.message)
            raise

    def get_job_info(self, job_id: str, user: User) -> BatchJobMetadata:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            metadata = con.job(backend_job_id).describe_job()
        metadata["id"] = job_id
        return BatchJobMetadata.from_dict(metadata)

    def start_job(self, job_id: str, user: 'User'):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).start_job()

    def cancel_job(self, job_id: str, user_id: str):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).stop_job()

    def delete_job(self, job_id: str, user_id: str):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).delete_job()

    def get_results(self, job_id: str, user_id: str) -> Dict[str, dict]:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            results = con.job(backend_job_id).get_results()
            assets = results.get_assets()
        return {a.name: {**a.metadata, **{BatchJobs.ASSET_PUBLIC_HREF: a.href}} for a in assets}

    def get_log_entries(self, job_id: str, user_id: str, offset: Optional[str] = None) -> List[dict]:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request), \
                self._translate_job_errors(job_id=job_id):
            params = dict_no_none(offset=offset)
            res = con.get(f"/jobs/{backend_job_id}/logs", params=params, expected_status=200).json()
        return res["logs"]


class AggregatorBackendImplementation(OpenEoBackendImplementation):
    def __init__(self, backends: MultiBackendConnection, config: AggregatorConfig):
        self._backends = backends
        catalog = AggregatorCollectionCatalog(backends=backends)
        processing = AggregatorProcessing(
            backends=backends, catalog=catalog,
            stream_chunk_size=config.streaming_chunk_size,
        )
        batch_jobs = AggregatorBatchJobs(backends=backends, processing=processing)
        super().__init__(
            catalog=catalog,
            processing=processing,
            secondary_services=None,
            batch_jobs=batch_jobs,
            user_defined_processes=None,
        )
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT)

    def oidc_providers(self) -> List[OidcProvider]:
        return self._backends.get_oidc_providers()

    def file_formats(self) -> dict:
        return self._cache.get_or_call(key="file_formats", callback=self._file_formats)

    def _file_formats(self) -> dict:
        input_formats = {}
        output_formats = {}
        for con in self._backends:
            try:
                file_formats = con.get("/file_formats").json()
            except Exception:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get file_formats from {con.id}", exc_info=True)
                continue
            # TODO smarter merging: case insensitive format name handling, parameter differences?
            input_formats.update(file_formats.get("input", {}))
            output_formats.update(file_formats.get("output", {}))
        return {"input": input_formats, "output": output_formats}
