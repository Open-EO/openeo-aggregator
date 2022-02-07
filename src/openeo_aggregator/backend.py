from collections import defaultdict

import contextlib
import flask
import functools
import logging
import time
from kazoo.client import KazooClient
from typing import List, Dict, Union, Tuple, Optional, Iterable, Iterator, Callable, Any

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError, OpenEoRestError, OpenEoClientException
from openeo.util import dict_no_none, TimingLogger, deep_get
from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT, CACHE_TTL_DEFAULT, \
    CONNECTION_TIMEOUT_RESULT, CONNECTION_TIMEOUT_JOB_START, ConfigException
from openeo_aggregator.connection import MultiBackendConnection, BackendConnection, streaming_flask_response
from openeo_aggregator.egi import is_early_adopter, is_free_tier
from openeo_aggregator.errors import BackendLookupFailureException
from openeo_aggregator.partitionedjobs import PartitionedJob
from openeo_aggregator.partitionedjobs.splitting import FlimsySplitter, TileGridSplitter
from openeo_aggregator.partitionedjobs.tracking import PartitionedJobConnection, PartitionedJobTracker
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB
from openeo_aggregator.utils import TtlCache, MultiDictGetter, subdict, dict_merge
from openeo_driver.ProcessGraphDeserializer import SimpleProcessing
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters, Processing, \
    OidcProvider, BatchJobs, BatchJobMetadata
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException, ProcessGraphMissingException, \
    JobNotFoundException, JobNotFinishedException, ProcessGraphInvalidException, PermissionsInsufficientException, \
    FeatureUnsupportedException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.users import User
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)


class _InternalCollectionMetadata:
    def __init__(self):
        self._data = {}

    def set_backends_for_collection(self, cid: str, backends: Iterable[str]):
        self._data.setdefault(cid, {})
        self._data[cid]["backends"] = list(backends)

    def get_backends_for_collection(self, cid: str) -> List[str]:
        if cid not in self._data:
            raise CollectionNotFoundException(collection_id=cid)
        return self._data[cid]["backends"]

    def list_backends_per_collection(self) -> Iterator[Tuple[str, List[str]]]:
        for cid, data in self._data.items():
            yield cid, data.get("backends", [])


class AggregatorCollectionCatalog(AbstractCollectionCatalog):
    # STAC property to use in collection "summaries" and user defined backend selection
    STAC_PROPERTY_PROVIDER_BACKEND = "provider:backend"

    def __init__(self, backends: MultiBackendConnection):
        self.backends = backends
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT, name="CollectionCatalog")
        self.backends.on_connections_change.add(self._cache.flush_all)

    def get_all_metadata(self) -> List[dict]:
        metadata, internal = self._get_all_metadata_cached()
        return metadata

    def _get_all_metadata_cached(self) -> Tuple[List[dict], _InternalCollectionMetadata]:
        return self._cache.get_or_call(key=("all",), callback=self._get_all_metadata, log_on_miss=True)

    def _get_all_metadata(self) -> Tuple[List[dict], _InternalCollectionMetadata]:
        """
        Get all collection metadata from all backends and combine.

        :return: tuple (metadata, internal), with `metadata`: combined collection metadata,
            `internal`: internal description of how backends and collections relate
        """
        # Group collection metadata by hierarchically: collection id -> backend id -> metadata
        grouped = defaultdict(dict)
        with TimingLogger(title="Collect collection metadata from all backends", logger=_log):
            for con in self.backends:
                try:
                    backend_collections = con.list_collections()
                except Exception as e:
                    # TODO: user warning https://github.com/Open-EO/openeo-api/issues/412
                    _log.warning(f"Failed to get collection metadata from {con.id}: {e!r}", exc_info=True)
                    # On failure: still cache, but with shorter TTL? (#2)
                    continue
                for collection_metadata in backend_collections:
                    if "id" in collection_metadata:
                        grouped[collection_metadata["id"]][con.id] = collection_metadata
                    else:
                        # TODO: there must be something seriously wrong with this backend: skip all its results?
                        _log.warning(f"Invalid collection metadata from {con.id}: %r", collection_metadata)

        # Merge down to single set of collection metadata
        collections_metadata = []
        internal_data = _InternalCollectionMetadata()
        for cid, by_backend in grouped.items():
            if len(by_backend) == 1:
                # Simple case: collection is only available on single backend.
                _log.debug(f"Accept single backend collection {cid} as is")
                (bid, metadata), = by_backend.items()
            else:
                _log.info(f"Merging {cid!r} collection metadata from backends {by_backend.keys()}")
                metadata = self._merge_collection_metadata(by_backend)
            collections_metadata.append(metadata)
            internal_data.set_backends_for_collection(cid, by_backend.keys())

        return collections_metadata, internal_data

    def _merge_collection_metadata(self, by_backend: Dict[str, dict]) -> dict:
        """
        Merge collection metadata dicts from multiple backends
        """
        getter = MultiDictGetter(by_backend.values())

        ids = set(getter.get("id"))
        if len(ids) != 1:
            raise ValueError(f"Single collection id expected, but got {ids}")
        cid = ids.pop()
        _log.info(f"Merging collection metadata for {cid!r}")

        result = {
            "id": cid,
        }

        result["stac_version"] = max(list(getter.get("stac_version")) + ["0.9.0"])
        stac_extensions = sorted(getter.union("stac_extensions", skip_duplicates=True))
        if stac_extensions:
            result["stac_extensions"] = stac_extensions
        # TODO: better merging of title and description?
        result["title"] = getter.first("title", default=result["id"])
        result["description"] = getter.first("description", default=result["id"])
        keywords = getter.union("keywords", skip_duplicates=True)
        if keywords:
            result["keywords"] = keywords
        versions = set(getter.get("version"))
        if versions:
            # TODO: smarter version maximum?
            result["version"] = max(versions)
        deprecateds = list(getter.get("deprecated"))
        if deprecateds:
            result["deprecated"] = all(deprecateds)
        licenses = set(getter.get("license"))
        result["license"] = licenses.pop() if len(licenses) == 1 else ("various" if licenses else "proprietary")
        # TODO: .. "various" if multiple licenses apply ... links to the license texts SHOULD be added,
        providers = getter.union("providers", skip_duplicates=True)
        if providers:
            result["providers"] = list(providers)
        result["extent"] = {
            "spatial": {
                "bbox": getter.select("extent").select("spatial").union("bbox", skip_duplicates=True) \
                        or [[-180, -90, 180, 90]],
            },
            "temporal": {
                "interval": getter.select("extent").select("temporal").union("interval", skip_duplicates=True) \
                            or [[None, None]],
            },
        }
        result["links"] = list(getter.union("links"))
        # TODO: combine cube:dimensions smarter?
        cube_dimensions = getter.first("cube:dimensions")
        if cube_dimensions:
            result["cube:dimensions"] = cube_dimensions
        # TODO merge existing summaries?
        result["summaries"] = {
            # TODO: use a more robust/user friendly backend pointer than backend id (which is internal implementation detail)
            self.STAC_PROPERTY_PROVIDER_BACKEND: list(by_backend.keys())
        }
        # TODO: assets ?

        return result

    @staticmethod
    def generate_backend_constraint_callables(process_graphs: Iterable[dict]) -> List[Callable[[str], bool]]:
        """
        Convert a collection of process graphs (implementing a condition that works on backend id)
        to a list of Python functions.
        """
        processing = SimpleProcessing()
        env = processing.get_basic_env()

        def evaluate(backend_id, pg):
            return processing.evaluate(
                process_graph=pg,
                env=env.push(parameters={"value": backend_id})
            )

        return [functools.partial(evaluate, pg=pg) for pg in process_graphs]

    def get_backend_candidates_for_collections(self, collections: Iterable[str]) -> List[str]:
        """
        Get best backend id providing all given collections
        :param collections: list/set of collection ids
        :return:
        """
        metadata, internal = self._get_all_metadata_cached()

        # cid -> tuple of backends that provide it
        collection_backends_map = {cid: tuple(backends) for cid, backends in internal.list_backends_per_collection()}

        try:
            backend_combos = set(collection_backends_map[cid] for cid in collections)
        except KeyError as e:
            raise CollectionNotFoundException(collection_id=e.args[0])

        if len(backend_combos) == 0:
            raise BackendLookupFailureException("Empty collection set given")
        elif len(backend_combos) == 1:
            backend_candidates = list(backend_combos.pop())
        else:
            # Search for common backends in all sets (and preserve order)
            intersection = functools.reduce(lambda a, b: [x for x in a if x in b], backend_combos)
            if intersection:
                backend_candidates = list(intersection)
            else:
                union = functools.reduce(lambda a, b: set(a).union(b), backend_combos)
                raise BackendLookupFailureException(
                    message=f"Collections across multiple backends ({union}): {collections}."
                )

        _log.info(f"Backend candidates {backend_candidates} for collections {collections}")
        return backend_candidates

    def get_collection_metadata(self, collection_id: str) -> dict:
        return self._cache.get_or_call(
            key=("collection", collection_id),
            callback=lambda: self._get_collection_metadata(collection_id),
            log_on_miss=True,
        )

    def _get_collection_metadata(self, collection_id: str) -> dict:
        # Get backend ids that support this collection
        metadata, internal = self._get_all_metadata_cached()
        backends = internal.get_backends_for_collection(collection_id)

        by_backend = {}
        for bid in backends:
            con = self.backends.get_connection(backend_id=bid)
            try:
                by_backend[bid] = con.describe_collection(name=collection_id)
            except Exception as e:
                # TODO: user warning https://github.com/Open-EO/openeo-api/issues/412
                _log.warning(f"Failed collection metadata for {collection_id!r} at {con.id}: {e!r}", exc_info=True)
                # TODO: avoid caching of final result? (#2)
                continue

        if len(by_backend) == 0:
            raise CollectionNotFoundException(collection_id=collection_id)
        elif len(by_backend) == 1:
            # TODO: also go through _merge_collection_metadata procedure (for clean up/normalization)?
            return by_backend.popitem()[1]
        else:
            _log.info(f"Merging metadata for collection {collection_id}.")
            return self._merge_collection_metadata(by_backend=by_backend)

    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> DriverDataCube:
        raise RuntimeError("openeo-aggregator does not implement concrete collection loading")

    def get_collection_items(self, collection_id: str, parameters: dict) -> Union[dict, flask.Response]:
        metadata, internal = self._get_all_metadata_cached()
        backends = internal.get_backends_for_collection(collection_id)

        if len(backends) == 1:
            con = self.backends.get_connection(backend_id=backends[0])
            resp = con.get(f"/collections/{collection_id}/items", params=parameters, stream=True)
            return streaming_flask_response(resp)
        elif len(backends) > 1:
            raise FeatureUnsupportedException(f"collection-items with multiple backends ({backends}) is not supported.")
        else:
            raise CollectionNotFoundException(collection_id)


class JobIdMapping:
    """Mapping between aggregator job ids and backend job ids"""

    # Job-id prefix for jobs managed by aggregator (not simple proxied jobs)
    AGG = "agg"

    @staticmethod
    def get_aggregator_job_id(backend_job_id: str, backend_id: str) -> str:
        """Construct aggregator job id from given backend job id and backend id"""
        return f"{backend_id}-{backend_job_id}"

    @classmethod
    def parse_aggregator_job_id(cls, backends: MultiBackendConnection, aggregator_job_id: str) -> Tuple[str, str]:
        """Given aggregator job id: extract backend job id and backend id"""
        for prefix in [f"{con.id}-" for con in backends] + [cls.AGG + "-"]:
            if aggregator_job_id.startswith(prefix):
                backend_id, backend_job_id = aggregator_job_id.split("-", maxsplit=1)
                return backend_job_id, backend_id
        raise JobNotFoundException(job_id=aggregator_job_id)


class AggregatorProcessing(Processing):
    def __init__(
            self,
            backends: MultiBackendConnection,
            catalog: AggregatorCollectionCatalog,
            stream_chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT
    ):
        self.backends = backends
        # TODO Cache per backend results instead of output?
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT, name="Processing")
        self.backends.on_connections_change.add(self._cache.flush_all)
        self._catalog = catalog
        self._stream_chunk_size = stream_chunk_size

    def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
        if api_version != self.backends.api_version:
            raise OpenEOApiException(
                message=f"Requested API version {api_version} != expected {self.backends.api_version}"
            )
        return self._cache.get_or_call(key=str(api_version), callback=self._get_process_registry, log_on_miss=True)

    def _get_process_registry(self) -> ProcessRegistry:
        processes_per_backend = {}
        for con in self.backends:
            try:
                processes_per_backend[con.id] = {p["id"]: p for p in con.list_processes()}
            except Exception as e:
                # TODO: user warning https://github.com/Open-EO/openeo-api/issues/412
                _log.warning(f"Failed to get processes from {con.id}: {e!r}", exc_info=True)

        # TODO #4: combined set of processes: union, intersection or something else?
        # TODO #4: not only check process name, but also parameters and return type?
        combined_processes = {}
        for bid, backend_processes in processes_per_backend.items():
            # Combine by taking union (with higher preference for earlier backends)
            combined_processes = {**backend_processes, **combined_processes}

        process_registry = ProcessRegistry()
        for pid, spec in combined_processes.items():
            process_registry.add_spec(spec=spec)

        return process_registry

    def get_backend_for_process_graph(self, process_graph: dict, api_version: str) -> str:
        """
        Get backend capable of executing given process graph (based on used collections, processes, ...)
        """
        # Initial list of candidates
        backend_candidates: List[str] = [b.id for b in self.backends]

        # TODO: also check used processes?
        collections = set()
        collection_backend_constraints = []
        try:
            for pg_node in process_graph.values():
                process_id = pg_node["process_id"]
                arguments = pg_node["arguments"]
                if process_id == "load_collection":
                    collections.add(arguments["id"])
                    provider_backend_pg = deep_get(
                        arguments, "properties", self._catalog.STAC_PROPERTY_PROVIDER_BACKEND, "process_graph",
                        default=None
                    )
                    if provider_backend_pg:
                        collection_backend_constraints.append(provider_backend_pg)
                elif process_id == "load_result":
                    # Extract backend id that has this batch job result to load
                    _, job_backend_id = JobIdMapping.parse_aggregator_job_id(
                        backends=self.backends,
                        aggregator_job_id=arguments["id"]
                    )
                    backend_candidates = [b for b in backend_candidates if b == job_backend_id]
        except Exception as e:
            _log.error(f"Failed to parse process graph: {e!r}", exc_info=True)
            raise ProcessGraphInvalidException()

        if collections:
            # Determine backend candidates based on collections used in load_collection processes.
            collection_candidates = self._catalog.get_backend_candidates_for_collections(collections=collections)
            backend_candidates = [b for b in backend_candidates if b in collection_candidates]

            if collection_backend_constraints:
                conditions = self._catalog.generate_backend_constraint_callables(
                    process_graphs=collection_backend_constraints
                )
                backend_candidates = [b for b in backend_candidates if all(c(b) for c in conditions)]

        if not backend_candidates:
            raise BackendLookupFailureException(message="No backend matching all constraints")
        return backend_candidates[0]

    def evaluate(self, process_graph: dict, env: EvalEnv = None):
        """Evaluate given process graph (flat dict format)."""

        backend_id = self.get_backend_for_process_graph(process_graph, api_version=env.get("version"))

        # Preprocess process graph (e.g. translate job ids and other references)
        process_graph = self.preprocess_process_graph(process_graph, backend_id=backend_id)

        # Send process graph to backend
        con = self.backends.get_connection(backend_id=backend_id)
        request_pg = {"process": {"process_graph": process_graph}}
        timing_logger = TimingLogger(title=f"Evaluate process graph on backend {backend_id}", logger=_log.info)
        with con.authenticated_from_request(flask.request), timing_logger:
            try:
                backend_response = con.post(
                    path="/result", json=request_pg,
                    stream=True, timeout=CONNECTION_TIMEOUT_RESULT,
                    expected_status=200,
                )
            except Exception as e:
                _log.error(f"Failed to process synchronously on backend {con.id}: {e!r}", exc_info=True)
                raise OpenEOApiException(message=f"Failed to process synchronously on backend {con.id}: {e!r}")

        return streaming_flask_response(backend_response, chunk_size=self._stream_chunk_size)

    def preprocess_process_graph(self, process_graph: dict, backend_id: str) -> dict:
        def preprocess(node: Any) -> Any:
            if isinstance(node, dict):
                if "process_id" in node and "arguments" in node:
                    process_id = node["process_id"]
                    arguments = node["arguments"]
                    if process_id == "load_result" and "id" in arguments:
                        job_id, job_backend_id = JobIdMapping.parse_aggregator_job_id(
                            backends=self.backends,
                            aggregator_job_id=arguments["id"]
                        )
                        assert job_backend_id == backend_id, f"{job_backend_id} != {backend_id}"
                        # Create new load_result node dict with updated job id
                        return dict_merge(node, arguments=dict_merge(arguments, id=job_id))
                return {k: preprocess(v) for k, v in node.items()}
            elif isinstance(node, list):
                return [preprocess(x) for x in node]
            return node

        return preprocess(process_graph)


class AggregatorBatchJobs(BatchJobs):

    def __init__(
            self,
            backends: MultiBackendConnection,
            processing: AggregatorProcessing,
            partitioned_job_tracker: Optional[PartitionedJobTracker] = None,
    ):
        super(AggregatorBatchJobs, self).__init__()
        self.backends = backends
        self.processing = processing
        self.partitioned_job_tracker = partitioned_job_tracker

    def get_user_jobs(self, user_id: str) -> Union[List[BatchJobMetadata], dict]:
        jobs = []
        federation_missing = set()
        for con in self.backends:
            with con.authenticated_from_request(request=flask.request, user=User(user_id)), \
                    TimingLogger(f"get_user_jobs: {con.id}", logger=_log.debug):
                try:
                    backend_jobs = con.list_jobs()
                except Exception as e:
                    # TODO: user warning https://github.com/Open-EO/openeo-api/issues/412
                    _log.warning(f"Failed to get job listing from backend {con.id!r}: {e!r}")
                    federation_missing.add(con.id)
                    backend_jobs = []
                for job in backend_jobs:
                    job["id"] = JobIdMapping.get_aggregator_job_id(backend_job_id=job["id"], backend_id=con.id)
                    jobs.append(BatchJobMetadata.from_api_dict(job))

        if self.partitioned_job_tracker:
            for job in self.partitioned_job_tracker.list_user_jobs(user_id=user_id):
                job["id"] = JobIdMapping.get_aggregator_job_id(backend_job_id=job["id"], backend_id=JobIdMapping.AGG)
                jobs.append(BatchJobMetadata.from_api_dict(job))

        federation_missing.update(self.backends.get_disabled_connection_ids())
        return dict_no_none({
            "jobs": jobs,
            # TODO: experimental "federation:missing" https://github.com/openEOPlatform/architecture-docs/issues/179
            "federation:missing": list(federation_missing) or None
        })

    def create_job(
            self, user_id: str, process: dict, api_version: str,
            metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        if "process_graph" not in process:
            raise ProcessGraphMissingException()

        # TODO: better, more generic/specific job_option(s)?
        if job_options and (job_options.get("_jobsplitting") or job_options.get("tile_grid")):
            return self._create_partitioned_job(
                user_id=user_id,
                process=process,
                api_version=api_version,
                metadata=metadata,
                job_options=job_options,
            )
        else:
            return self._create_job_standard(
                user_id=user_id,
                process_graph=process["process_graph"],
                api_version=api_version,
                metadata=metadata,
                job_options=job_options,
            )

    def _create_job_standard(
            self, user_id: str, process_graph: dict, api_version: str, metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        """Standard batch job creation: just proxy to a single batch job on single back-end."""
        backend_id = self.processing.get_backend_for_process_graph(
            process_graph=process_graph, api_version=api_version
        )
        process_graph = self.processing.preprocess_process_graph(process_graph, backend_id=backend_id)

        con = self.backends.get_connection(backend_id)
        with con.authenticated_from_request(request=flask.request, user=User(user_id=user_id)), \
                con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START):
            try:
                job = con.create_job(
                    process_graph=process_graph,
                    title=metadata.get("title"), description=metadata.get("description"),
                    plan=metadata.get("plan"), budget=metadata.get("budget"),
                    additional=job_options,
                )
            except OpenEoApiError as e:
                for exc_class in [ProcessGraphMissingException, ProcessGraphInvalidException]:
                    if e.code == exc_class.code:
                        raise exc_class
                raise OpenEOApiException(f"Failed to create job on backend {backend_id!r}: {e!r}")
            except (OpenEoRestError, OpenEoClientException) as e:
                raise OpenEOApiException(f"Failed to create job on backend {backend_id!r}: {e!r}")
        return BatchJobMetadata(
            id=JobIdMapping.get_aggregator_job_id(backend_job_id=job.job_id, backend_id=backend_id),
            # Note: required, but unused metadata
            status="dummy", created="dummy", process={"dummy": "dummy"}
        )

    def _create_partitioned_job(
            self, user_id: str, process: dict, api_version: str, metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        """
        Advanced/handled batch job creation:
        split original job in (possibly) multiple sub-jobs,
        distribute across (possibly) multiple back-ends
        and keep track of them.
        """
        if not self.partitioned_job_tracker:
            raise FeatureUnsupportedException(message="Partitioned job tracking is not supported")

        if "tile_grid" in job_options:
            splitter = TileGridSplitter(processing=self.processing)
        else:
            splitter = FlimsySplitter(processing=self.processing)
        pjob: PartitionedJob = splitter.split(process=process, metadata=metadata, job_options=job_options)

        job_id = self.partitioned_job_tracker.create(user_id=user_id, pjob=pjob, flask_request=flask.request)

        return BatchJobMetadata(
            id=JobIdMapping.get_aggregator_job_id(backend_job_id=job_id, backend_id=JobIdMapping.AGG),
            status="dummy", created="dummy", process={"dummy": "dummy"}
        )

    def _get_connection_and_backend_job_id(
            self,
            aggregator_job_id: str
    ) -> Tuple[Union[BackendConnection, PartitionedJobConnection], str]:
        backend_job_id, backend_id = JobIdMapping.parse_aggregator_job_id(
            backends=self.backends,
            aggregator_job_id=aggregator_job_id
        )

        if backend_id == JobIdMapping.AGG and self.partitioned_job_tracker:
            return PartitionedJobConnection(self.partitioned_job_tracker), backend_job_id

        con = self.backends.get_connection(backend_id)
        return con, backend_job_id

    @contextlib.contextmanager
    def _translate_job_errors(self, job_id):
        """Context manager to translate job related errors, where necessary"""
        try:
            yield
        except OpenEoApiError as e:
            if e.code == JobNotFoundException.code:
                raise JobNotFoundException(job_id=job_id)
            elif e.code == JobNotFinishedException.code:
                raise JobNotFinishedException(message=e.message)
            raise

    def get_job_info(self, job_id: str, user: User) -> BatchJobMetadata:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=user), \
                self._translate_job_errors(job_id=job_id):
            metadata = con.job(backend_job_id).describe_job()
        metadata["id"] = job_id
        return BatchJobMetadata.from_api_dict(metadata)

    def start_job(self, job_id: str, user: User):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=user), \
                con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).start_job()

    def cancel_job(self, job_id: str, user_id: str):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=User(user_id)), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).stop_job()

    def delete_job(self, job_id: str, user_id: str):
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=User(user_id)), \
                self._translate_job_errors(job_id=job_id):
            con.job(backend_job_id).delete_job()

    def get_results(self, job_id: str, user_id: str) -> Dict[str, dict]:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=User(user_id)), \
                self._translate_job_errors(job_id=job_id):
            results = con.job(backend_job_id).get_results()
            assets = results.get_assets()
        return {a.name: {**a.metadata, **{BatchJobs.ASSET_PUBLIC_HREF: a.href}} for a in assets}

    def get_log_entries(self, job_id: str, user_id: str, offset: Optional[str] = None) -> List[dict]:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        with con.authenticated_from_request(request=flask.request, user=User(user_id)), \
                self._translate_job_errors(job_id=job_id):
            return con.job(backend_job_id).logs(offset=offset)


class AggregatorBackendImplementation(OpenEoBackendImplementation):
    # No basic auth: OIDC auth is required (to get EGI Check-in eduperson_entitlement data)
    enable_basic_auth = False

    # Simplify mocking time for unit tests.
    _clock = time.time  # TODO: centralized helper for this test pattern

    def __init__(self, backends: MultiBackendConnection, config: AggregatorConfig):
        self._backends = backends
        catalog = AggregatorCollectionCatalog(backends=backends)
        processing = AggregatorProcessing(
            backends=backends, catalog=catalog,
            stream_chunk_size=config.streaming_chunk_size,
        )

        if config.partitioned_job_tracking:
            partitioned_job_tracker = PartitionedJobTracker.from_config(config=config, backends=self._backends)
        else:
            partitioned_job_tracker = None

        batch_jobs = AggregatorBatchJobs(
            backends=backends,
            processing=processing,
            partitioned_job_tracker=partitioned_job_tracker
        )
        super().__init__(
            catalog=catalog,
            processing=processing,
            secondary_services=None,
            batch_jobs=batch_jobs,
            user_defined_processes=None,
        )
        self._cache = TtlCache(default_ttl=CACHE_TTL_DEFAULT, name="General")
        self._backends.on_connections_change.add(self._cache.flush_all)
        self._auth_entitlement_check: Union[bool, dict] = config.auth_entitlement_check

    def oidc_providers(self) -> List[OidcProvider]:
        # TODO: openeo-python-driver (HttpAuthHandler) currently does support changes in
        #       the set of oidc_providers ids (id mapping is statically established at startup time)
        return self._cache.get_or_call(
            key="oidc_providers", callback=self._backends.get_oidc_providers, log_on_miss=True,
        )

    def file_formats(self) -> dict:
        return self._cache.get_or_call(key="file_formats", callback=self._file_formats, log_on_miss=True)

    def _file_formats(self) -> dict:
        input_formats = {}
        output_formats = {}

        def merge(formats: dict, to_add: dict):
            # TODO: merge parameters in some way?
            for name, data in to_add.items():
                if name.lower() not in {k.lower() for k in formats.keys()}:
                    formats[name] = data

        for con in self._backends:
            try:
                file_formats = con.get("/file_formats").json()
            except Exception as e:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get file_formats from {con.id}: {e!r}", exc_info=True)
                continue
            # TODO #1 smarter merging:  parameter differences?
            merge(input_formats, file_formats.get("input", {}))
            merge(output_formats, file_formats.get("output", {}))
        return {"input": input_formats, "output": output_formats}

    def user_access_validation(self, user: User, request: flask.Request) -> User:
        if self._auth_entitlement_check:
            int_data = user.internal_auth_data
            issuer_whitelist = self._auth_entitlement_check.get("oidc_issuer_whitelist", {"https://aai.egi.eu/oidc"})
            if not (
                    int_data["authentication_method"] == "OIDC"
                    and int_data["oidc_issuer"].rstrip("/").lower() in issuer_whitelist
            ):
                user_message = "An EGI account is required for using openEO Platform."
                _log.warning(f"user_access_validation failure: %r %r", user_message, {
                    "internal_auth_data": subdict(int_data, keys=["authentication_method", "oidc_issuer"]),
                    "issuer_whitelist": issuer_whitelist,
                })
                raise PermissionsInsufficientException(user_message)

            enrollment_error_user_message = "Proper enrollment in openEO Platform virtual organization is required."
            try:
                eduperson_entitlements = user.info["oidc_userinfo"]["eduperson_entitlement"]
            except KeyError as e:
                _log.warning(f"user_access_validation failure: %r %r", enrollment_error_user_message, {
                    "exception": repr(e),
                    # Note: just log userinfo keys to avoid leaking sensitive user data.
                    "userinfo keys": (user.info.keys(), user.info.get('oidc_userinfo', {}).keys())
                })
                raise PermissionsInsufficientException(enrollment_error_user_message)
            if any(is_early_adopter(e) for e in eduperson_entitlements):
                # TODO: list multiple roles/levels? Better "status" signaling?
                user.info["roles"] = ["EarlyAdopter"]
                user.info["default_plan"] = self.BILLING_PLAN_EARLY_ADOPTER
            elif any(is_free_tier(e) for e in eduperson_entitlements):
                user.info["roles"] = ["FreeTier"]
                user.info["default_plan"] = self.BILLING_PLAN_FREE
            else:
                _log.warning(f"user_access_validation failure: %r %r", enrollment_error_user_message, {
                    "user_id": user.user_id,
                    "eduperson_entitlements": eduperson_entitlements
                })
                raise PermissionsInsufficientException(enrollment_error_user_message)

        return user

    def health_check(self) -> Union[str, dict, flask.Response]:
        backend_status = {}
        overall_status_code = 200
        for con in self._backends:
            backend_status[con.id] = {}
            start_time = self._clock()
            try:
                # TODO: this `/health` endpoint is not standardized. Get it from `aggregator_backends` config?
                resp = con.get("/health", check_error=False, timeout=10)
                backend_status[con.id]["status_code"] = resp.status_code
                backend_status[con.id]["response_time"] = self._clock() - start_time
                if resp.status_code >= 400:
                    overall_status_code = max(overall_status_code, resp.status_code)
                if resp.headers.get("Content-type") == "application/json":
                    backend_status[con.id]["json"] = resp.json()
                else:
                    backend_status[con.id]["text"] = resp.text
            except Exception as e:
                backend_status[con.id]["error"] = repr(e)
                backend_status[con.id]["error_time"] = self._clock() - start_time
                overall_status_code = 500

        response = flask.jsonify({
            "status_code": overall_status_code,
            "backend_status": backend_status,
        })
        response.status_code = overall_status_code
        return response

    BILLING_PLAN_FREE = "free"
    BILLING_PLAN_EARLY_ADOPTER = "early-adopter"
    BILLING_PLAN_VSC4 = "vsc4"

    def capabilities_billing(self) -> dict:
        # TODO: ok to hardcode this here, or move to config?
        return {
            "currency": "EUR",
            "plans": [
                {
                    "name": self.BILLING_PLAN_EARLY_ADOPTER,
                    "description": "openEO.cloud early adopter plan",
                    "url": "https://openeo.cloud/early-adopters/",
                    "paid": True,
                },
                # TODO: Unused plan at the moment: necessary to expose it?
                {
                    "name": self.BILLING_PLAN_FREE,
                    "description": "openEO.cloud free plan (experimental)",
                    # TODO: url?
                    "paid": False
                },
                # TODO: Unused plan at the moment: necessary to expose it?
                {
                    "name": self.BILLING_PLAN_VSC4,
                    "description": "[EODC] Use this plan to process data on VSC-4",
                    # TODO: url?
                    "paid": True,
                }
            ]
        }

    def postprocess_capabilities(self, capabilities: dict) -> dict:
        # TODO: which url to use? unversioned or versioned? see https://github.com/Open-EO/openeo-api/pull/419
        capabilities["federation"] = {
            bid: {
                "url": status["root_url"],
            }
            for bid, status in self._backends.get_status().items()
        }
        # TODO: standardize this field?
        capabilities["_partitioned_job_tracking"] = bool(self.batch_jobs.partitioned_job_tracker)
        return capabilities
