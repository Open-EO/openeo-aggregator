import contextlib
import datetime
import functools
import itertools
import logging
import time
from collections import defaultdict
from typing import List, Dict, Union, Tuple, Optional, Iterable, Iterator, Callable, Any

import flask

import openeo_driver.util.view_helpers
from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoApiError, OpenEoRestError, OpenEoClientException
from openeo.util import dict_no_none, TimingLogger, deep_get, rfc3339
from openeo_aggregator.caching import memoizer_from_config, Memoizer, json_serde
from openeo_aggregator.config import AggregatorConfig, CONNECTION_TIMEOUT_RESULT, CONNECTION_TIMEOUT_JOB_START
from openeo_aggregator.connection import MultiBackendConnection, BackendConnection, streaming_flask_response
from openeo_aggregator.egi import is_early_adopter, is_30day_trial
from openeo_aggregator.errors import BackendLookupFailureException
from openeo_aggregator.partitionedjobs import PartitionedJob
from openeo_aggregator.partitionedjobs.splitting import FlimsySplitter, TileGridSplitter
from openeo_aggregator.partitionedjobs.tracking import PartitionedJobConnection, PartitionedJobTracker
from openeo_aggregator.utils import MultiDictGetter, subdict, dict_merge, normalize_issuer_url
from openeo_driver.ProcessGraphDeserializer import SimpleProcessing
from openeo_driver.backend import OpenEoBackendImplementation, AbstractCollectionCatalog, LoadParameters, Processing, \
    OidcProvider, BatchJobs, BatchJobMetadata, SecondaryServices, ServiceMetadata
from openeo_driver.datacube import DriverDataCube
from openeo_driver.errors import CollectionNotFoundException, OpenEOApiException, ProcessGraphMissingException, \
    JobNotFoundException, JobNotFinishedException, ProcessGraphInvalidException, PermissionsInsufficientException, \
    FeatureUnsupportedException, ServiceNotFoundException
from openeo_driver.processes import ProcessRegistry
from openeo_driver.users import User
from openeo_driver.utils import EvalEnv

_log = logging.getLogger(__name__)


@json_serde.register_custom_codec
class _InternalCollectionMetadata:
    def __init__(self, data: Optional[Dict[str, dict]] = None):
        self._data = data or {}

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

    def __jsonserde_prepare__(self) -> dict:
        return self._data

    @classmethod
    def __jsonserde_load__(cls, data: dict):
        return cls(data=data)


class AggregatorCollectionCatalog(AbstractCollectionCatalog):
    # STAC property to use in collection "summaries" and user defined backend selection
    # TODO: is this the proper standard field to use? Also see https://github.com/openEOPlatform/architecture-docs/issues/268
    STAC_PROPERTY_PROVIDER_BACKEND = "provider:backend"

    def __init__(self, backends: MultiBackendConnection, config: AggregatorConfig):
        self.backends = backends
        self._memoizer = memoizer_from_config(config=config, namespace="CollectionCatalog")
        self.backends.on_connections_change.add(self._memoizer.invalidate)

    def get_all_metadata(self) -> List[dict]:
        metadata, internal = self._get_all_metadata_cached()
        return metadata

    def _get_all_metadata_cached(self) -> Tuple[List[dict], _InternalCollectionMetadata]:
        return self._memoizer.get_or_call(key=("all",), callback=self._get_all_metadata)

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
                        # TODO: support a trigger to create a collection alias under other name?
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
                try:
                    metadata = self._merge_collection_metadata(by_backend)
                except Exception as e:
                    _log.error(f"Failed to merge collection metadata for {cid!r}", exc_info=True)
                    continue
            metadata = self._normalize_metadata(metadata)
            collections_metadata.append(metadata)
            internal_data.set_backends_for_collection(cid, by_backend.keys())

        return collections_metadata, internal_data

    @classmethod
    def _normalize_metadata(cls, metadata: dict) -> dict:
        cid = metadata.get("id", None)
        if cid is None:
            raise OpenEOApiException("Missing collection id in metadata")
        if "links" not in metadata:
            metadata["links"] = []
        metadata["links"] = [l for l in metadata["links"] if l.get("rel") not in ("self", "parent", "root")]
        if flask.has_app_context():
            metadata["links"].append({
                "href": flask.url_for("openeo.collections", _external=True),
                "rel": "root"
            })
            metadata["links"].append({
                "href": flask.url_for("openeo.collections", _external=True),
                "rel": "parent"
            })
            metadata["links"].append({
                "href": flask.url_for("openeo.collection_by_id", collection_id=cid, _external=True),
                "rel": "self"
            })
        else:
            _log.warning("Unable to provide root/parent/self links in collection metadata outside flask app context")
        return metadata

    @classmethod
    def _merge_collection_metadata(cls, by_backend: Dict[str, dict], report=_log.warning) -> dict:
        """
        Merge collection metadata dicts from multiple backends
        """
        getter = MultiDictGetter(by_backend.values())

        ids = set(getter.get("id"))
        if len(ids) != 1:
            raise ValueError(f"Single collection id expected, but got {ids}")
        cid = ids.pop()
        _log.info(f"Merging collection metadata for {cid!r}")

        # Start with some initial/required fields
        result = {
            "id": cid,
            "stac_version": max(list(getter.get("stac_version")) + ["0.9.0"]),
            "title": getter.first("title", default=cid),
            "description": getter.first("description", default=cid),
            "type": getter.first("type", default="Collection"),
            "links": [
                k for k in getter.concat("links")
                # TODO: report invalid links (e.g. string instead of dict)
                if isinstance(k, dict) and k.get("rel") not in ("self", "parent", "root")
            ],
        }

        # Generic field merging
        # Notes:
        # - `crs` is required by OGC API: https://docs.opengeospatial.org/is/18-058/18-058.html#_crs_identifier_list
        # - `sci:doi` and related are defined at https://github.com/stac-extensions/scientific
        for field in getter.available_keys(["stac_extensions", "keywords", "providers", "sci:publications"]):
            result[field] = getter.concat(field, skip_duplicates=True)
        for field in getter.available_keys(["deprecated"]):
            result[field] = all(getter.get(field))
        for field in getter.available_keys(["crs", "sci:citation", "sci:doi"]):
            result[field] = getter.first(field)

        # Summary merging
        result["summaries"] = {}
        summaries_getter = getter.select("summaries")
        for summary_name in summaries_getter.keys():
            if summary_name in [
                "constellation", "platform", "instruments",
            ]:
                result["summaries"][summary_name] = summaries_getter.concat(summary_name, skip_duplicates=True)
            elif summary_name.startswith("sar:") or summary_name.startswith("sat:"):
                result["summaries"][summary_name] = summaries_getter.concat(summary_name, skip_duplicates=True)
            else:
                report(f"Unhandled merging of summary {summary_name!r}")

        # Assets
        if getter.has_key("assets"):
            result["assets"] = {k: getter.select("assets").first(k) for k in getter.select("assets").keys()}

        # All keys with special merge handling.
        versions = set(getter.get("version"))
        if versions:
            # TODO: smarter version maximum? Low priority, versions key is not used in most backends.
            result["version"] = max(versions)
        licenses = set(getter.get("license"))
        result["license"] = licenses.pop() if len(licenses) == 1 else ("various" if licenses else "proprietary")

        result["extent"] = {
            "spatial": {
                "bbox": getter.select("extent").select("spatial").concat("bbox", skip_duplicates=True) \
                        or [[-180, -90, 180, 90]],
            },
            "temporal": {
                "interval": getter.select("extent").select("temporal").concat("interval", skip_duplicates=True) \
                            or [[None, None]],
            },
        }

        if getter.has_key("cube:dimensions"):
            cube_dim_getter = getter.select("cube:dimensions")
            result["cube:dimensions"] = {}

            # Spatial dimensions
            for dim in cube_dim_getter.available_keys(["x", "y"]):
                result["cube:dimensions"][dim] = cube_dim_getter.first(dim)
                # TODO: check consistency of step and reference_system?
                try:
                    bounds = cube_dim_getter.select(dim).concat("extent")
                    result["cube:dimensions"][dim]["extent"] = [min(bounds), max(bounds)]
                except Exception as e:
                    report(f"Failed to merge cube:dimensions.{dim}.extent: {e!r}")
            # Temporal dimension
            for dim in cube_dim_getter.available_keys(["t"]):
                result["cube:dimensions"][dim] = cube_dim_getter.first(dim)
                # TODO: check consistency of step?
                try:
                    t_starts = [e[0] for e in cube_dim_getter.select(dim).get("extent") if e[0]]
                    t_ends = [e[1] for e in cube_dim_getter.select(dim).get("extent") if e[1]]
                    result["cube:dimensions"][dim]["extent"] = [
                        min(rfc3339.normalize(t) for t in t_starts) if t_starts else None,
                        max(rfc3339.normalize(t) for t in t_ends) if t_ends else None
                    ]
                except Exception as e:
                    report(f"Failed to merge cube:dimensions.{dim}.extent: {e!r}")

            for dim in cube_dim_getter.available_keys(["bands"]):
                result["cube:dimensions"][dim] = cube_dim_getter.first(dim)
                try:
                    # Find common prefix of bands
                    # TODO: better approach? e.g. keep everything and rewrite process graphs on the fly?
                    bands_iterator = cube_dim_getter.select(dim).get("values")
                    prefix = next(bands_iterator)
                    for bands in bands_iterator:
                        prefix = [t[0] for t in itertools.takewhile(lambda t: t[0] == t[1], zip(prefix, bands))]
                        if bands != prefix:
                            report(f"Trimming bands {bands} to common prefix {prefix}")
                    if len(prefix) > 0:
                        result["cube:dimensions"][dim]["values"] = prefix
                    else:
                        report(f"Empty prefix for bands, falling back to first back-end's bands")
                except Exception as e:
                    report(f"Failed to merge cube:dimensions.{dim}.extent: {e!r}")

        # TODO: use a more robust/user friendly backend pointer than backend id (which is internal implementation detail)
        result["summaries"][cls.STAC_PROPERTY_PROVIDER_BACKEND] = list(by_backend.keys())

        ## Log warnings for improper metadata.
        # license => Log warning for collections without license links.
        # TODO: report invalid links
        license_links = [k for k in getter.concat("links") if isinstance(k, dict) and k.get("rel") == "license"]
        if result["license"] in ["various", "proprietary"] and not license_links:
            _log.warning(f"Missing license links for collection: {cid}")

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
        return self._memoizer.get_or_call(
            key=("collection", collection_id),
            callback=lambda: self._get_collection_metadata(collection_id),
        )

    def _get_collection_metadata(self, collection_id: str) -> dict:
        # Get backend ids that support this collection
        metadata, internal = self._get_all_metadata_cached()
        backends = internal.get_backends_for_collection(collection_id)

        by_backend = {}
        for bid in backends:
            con = self.backends.get_connection(backend_id=bid)
            try:
                by_backend[bid] = con.describe_collection(collection_id)
            except Exception as e:
                # TODO: user warning https://github.com/Open-EO/openeo-api/issues/412
                _log.warning(f"Failed collection metadata for {collection_id!r} at {con.id}: {e!r}", exc_info=True)
                # TODO: avoid caching of final result? (#2)
                continue

        if len(by_backend) == 0:
            raise CollectionNotFoundException(collection_id=collection_id)
        elif len(by_backend) == 1:
            metadata = by_backend.popitem()[1]
        else:
            _log.info(f"Merging metadata for collection {collection_id}.")
            metadata = self._merge_collection_metadata(by_backend=by_backend)
        return self._normalize_metadata(metadata)

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
            config: AggregatorConfig,
    ):
        self.backends = backends
        # TODO Cache per backend results instead of output?
        self._memoizer = memoizer_from_config(config=config, namespace="Processing")
        self.backends.on_connections_change.add(self._memoizer.invalidate)
        self._catalog = catalog
        self._stream_chunk_size = config.streaming_chunk_size

        # TODO #42 /validation support
        self.validate = None

    def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
        if api_version != self.backends.api_version:
            # TODO: only check for mismatch in major version?
            _log.warning(f"API mismatch: requested {api_version} != upstream {self.backends.api_version}")

        combined_processes = self._memoizer.get_or_call(
            key=("all", str(api_version)),
            callback=self._get_merged_process_meatadata
        )
        process_registry = ProcessRegistry()
        for pid, spec in combined_processes.items():
            process_registry.add_spec(spec=spec)
        return process_registry

    def _get_merged_process_meatadata(self) -> dict:
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
        return combined_processes

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
                elif process_id == "load_ml_model":
                    model_backend_id = self._process_load_ml_model(arguments)[0]
                    if model_backend_id:
                        backend_candidates = [b for b in backend_candidates if b == model_backend_id]
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

            if len(backend_candidates) > 1:
                # TODO #42 Check `/validation` instead of naively picking first one?
                _log.warning(
                    f"Multiple back-end candidates {backend_candidates} for collections {collections}."
                    f" Naively picking first one."
                )

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
                    if process_id == "load_ml_model":
                        model_id = self._process_load_ml_model(arguments, expected_backend=backend_id)[1]
                        if model_id:
                            return dict_merge(node, arguments=dict_merge(arguments, id=model_id))
                return {k: preprocess(v) for k, v in node.items()}
            elif isinstance(node, list):
                return [preprocess(x) for x in node]
            return node

        return preprocess(process_graph)

    def _process_load_ml_model(
            self, arguments: dict, expected_backend: Optional[str] = None
    ) -> Tuple[Union[str, None], str]:
        """Handle load_ml_model: detect/strip backend_id from model_id if it is a job_id"""
        model_id = arguments.get("id")
        if model_id and not model_id.startswith("http"):
            # TODO: load_ml_model's `id` could also be file path (see https://github.com/Open-EO/openeo-processes/issues/384)
            job_id, job_backend_id = JobIdMapping.parse_aggregator_job_id(
                backends=self.backends,
                aggregator_job_id=model_id
            )
            if expected_backend and job_backend_id != expected_backend:
                raise BackendLookupFailureException(f"{job_backend_id} != {expected_backend}")
            return job_backend_id, job_id
        return None, model_id


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
        if job_options and (job_options.get("split_strategy") or job_options.get("tile_grid")):
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
        elif job_options.get("split_strategy") == "flimsy":
            splitter = FlimsySplitter(processing=self.processing)
        else:
            raise ValueError("Could not determine splitting strategy from job options")
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

    def get_job_info(self, job_id: str, user_id: str) -> BatchJobMetadata:
        con, backend_job_id = self._get_connection_and_backend_job_id(aggregator_job_id=job_id)
        user = User(user_id=user_id)
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


class AggregatorSecondaryServices(SecondaryServices):
    """
    Aggregator implementation of the Secondary Services "microservice"
    https://openeo.org/documentation/1.0/developers/api/reference.html#tag/Secondary-Services
    """

    def __init__(
            self,
            backends: MultiBackendConnection,
            processing: AggregatorProcessing
    ):
        super(AggregatorSecondaryServices, self).__init__()
        self._backends = backends
        self._processing = processing

    def service_types(self) -> dict:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/list-service-types"""

        service_types = {}

        def merge(formats: dict, to_add: dict):
            for name, data in to_add.items():
                if name.lower() not in {k.lower() for k in formats.keys()}:
                    formats[name] = data

        # Collect all service types from the backends.
        for con in self._backends:
            try:
                types_to_add = con.get("/service_types").json()
            except Exception as e:
                # TODO: fail instead of warn?
                _log.warning(f"Failed to get service_types from {con.id}: {e!r}", exc_info=True)
                continue
            # TODO #1 smarter merging:  parameter differences?
            merge(service_types, types_to_add)

        return service_types

    def list_services(self, user_id: str) -> List[ServiceMetadata]:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/list-services"""
        # TODO: user_id is not used, how to authenticate when we use the BackendConnection?

        all_services = []
        def merge(services, to_add):
            # For now ignore the links
            services_to_add = to_add.get("services")
            if services_to_add:
                services_metadata = [ServiceMetadata.from_dict(s) for s in services_to_add]
                services.extend(services_metadata)

        # Collect all services from the backends.
        for con in self._backends:
            services_json = None
            try:
                services_json = con.get("/services").json()
            except Exception as e:
                _log.warning(f"Failed to get services from {con.id}: {e!r}", exc_info=True)
                continue

            if services_json:
                merge(all_services, services_json)

        return all_services

    def service_info(self, user_id: str, service_id: str) -> ServiceMetadata:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/describe-service"""
        # TODO: user_id is not used, how to authenticate when we use the BackendConnection?

        # TODO: can there ever be a service with the same ID in multiple back-ends? (For the same user)
        for con in self._backends:
            try:
                service_json = con.get(f"/services/{service_id}").json()
            except Exception as e:
                _log.debug(f"No service with ID={service_id} in backend with ID={con.id}: {e!r}", exc_info=True)
                continue
            else:
                return ServiceMetadata.from_dict(service_json)

        raise ServiceNotFoundException(service_id)

    def create_service(self, user_id: str, process_graph: dict, service_type: str, api_version: str,
                       configuration: dict) -> str:
        """
        https://openeo.org/documentation/1.0/developers/api/reference.html#operation/create-service
        """
        # TODO: user_id is not used, how to authenticate when we use the BackendConnection?
        # TODO: configuration is not used. What to do with it?

        backend_id = self._processing.get_backend_for_process_graph(
            process_graph=process_graph, api_version=api_version
        )
        process_graph = self._processing.preprocess_process_graph(process_graph, backend_id=backend_id)

        con = self._backends.get_connection(backend_id)
        try:
            # create_service can raise ServiceUnsupportedException and OpenEOApiException.
            service = con.create_service(graph=process_graph, type=service_type)

        # TODO: This exception handling was copy-pasted. What do we actually need here?
        except OpenEoApiError as e:
            for exc_class in [ProcessGraphMissingException, ProcessGraphInvalidException]:
                if e.code == exc_class.code:
                    raise exc_class
            raise OpenEOApiException(f"Failed to create secondary service on backend {backend_id!r}: {e!r}")
        except (OpenEoRestError, OpenEoClientException) as e:
            raise OpenEOApiException(f"Failed to create secondary service on backend {backend_id!r}: {e!r}")

        return service.service_id

    def _find_connection_with_service_id(self, service_id: str) -> BackendConnection:
        """Get connection for the backend that contains the service, return None if not found."""

        # Search all services on the backends.
        for con in self._backends:
            try:
                _ = con.get(f"/services/{service_id}")
            except OpenEoApiError as e:
                if e.http_status_code == 404:
                    # Expected error
                    _log.debug(f"No service with ID={service_id} in backend with ID={con.id}: {e!r}", exc_info=True)
                    continue
                else:
                    _log.warning(f"Failed to get service {service_id!r} from {con.id}: {e!r}", exc_info=True)
                    raise e
            except Exception as e:
                _log.warning(f"Failed to get service {service_id!r} from {con.id}: {e!r}", exc_info=True)
                raise e
            else:
                return con

        return None

    def remove_service(self, user_id: str, service_id: str) -> None:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/delete-service"""
        # TODO: user_id is not used, how to authenticate when we use the BackendConnection?

        con = self._find_connection_with_service_id(service_id)
        if not con:
            raise ServiceNotFoundException(service_id)

        try:
            con.delete(f"/services/{service_id}", expected_status=204)
        except (OpenEoApiError, OpenEOApiException) as e:
            # TODO: maybe we should just let these exception straight go to the caller without logging it here.
            # Logging it here seems prudent and more consistent with the handling of unexpected exceptions below.
            _log.warning(f"Failed to delete service {service_id!r} from {con.id}: {e!r}", exc_info=True)
            raise
        except Exception as e:
            _log.warning(f"Failed to delete service {service_id!r} from {con.id}: {e!r}", exc_info=True)
            raise OpenEOApiException(
                f"Failed to delete secondary service with id {service_id!r} on backend {con.id!r}: {e!r}"
            ) from e

    def update_service(self, user_id: str, service_id: str, process_graph: dict) -> None:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/update-service"""
        # TODO: user_id is not used, how to authenticate when we use the BackendConnection?

        con = self._find_connection_with_service_id(service_id)
        if not con:
            raise ServiceNotFoundException(service_id)

        api_version = self._backends.api_version
        try:
            key = "process_graph" if api_version < ComparableVersion((1, 0, 0)) else "process"
            con.patch(f"/services/{service_id}", json={key: process_graph}, expected_status=204)
        except (OpenEoApiError, OpenEOApiException) as e:
            # TODO: maybe we should just let these exception straight go to the caller without logging it here.
            # Logging it here seems prudent and more consistent with the handling of unexpected exceptions below.
            _log.warning(f"Failed to delete service {service_id!r} from {con.id}: {e!r}", exc_info=True)
            raise
        except Exception as e:
            _log.warning(f"Failed to delete service {service_id!r} from {con.id}: {e!r}", exc_info=True)
            raise OpenEOApiException(
                f"Failed to delete secondary service with id {service_id!r} on backend {con.id!r}: {e!r}"
            ) from e


class AggregatorBackendImplementation(OpenEoBackendImplementation):
    # No basic auth: OIDC auth is required (to get EGI Check-in eduperson_entitlement data)
    enable_basic_auth = False

    # Simplify mocking time for unit tests.
    _clock = time.time  # TODO: centralized helper for this test pattern

    def __init__(self, backends: MultiBackendConnection, config: AggregatorConfig):
        self._backends = backends
        catalog = AggregatorCollectionCatalog(backends=backends, config=config)
        processing = AggregatorProcessing(
            backends=backends, catalog=catalog,
            config=config,
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

        secondary_services = AggregatorSecondaryServices(backends=backends, processing=processing)

        super().__init__(
            catalog=catalog,
            processing=processing,
            secondary_services=secondary_services,
            batch_jobs=batch_jobs,
            user_defined_processes=None,
        )
        self._configured_oidc_providers: List[OidcProvider] = config.configured_oidc_providers
        self._auth_entitlement_check: Union[bool, dict] = config.auth_entitlement_check

        self._memoizer: Memoizer = memoizer_from_config(config=config, namespace="general")
        self._backends.on_connections_change.add(self._memoizer.invalidate)

        # Shorter HTTP cache TTL to adapt quicker to changed back-end configurations
        self.cache_control = openeo_driver.util.view_helpers.cache_control(
            max_age=datetime.timedelta(minutes=15), public=True,
        )

    def oidc_providers(self) -> List[OidcProvider]:
        return self._configured_oidc_providers

    def file_formats(self) -> dict:
        return self._memoizer.get_or_call(key="file_formats", callback=self._file_formats)

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
            issuer_whitelist = [
                normalize_issuer_url(u)
                for u in self._auth_entitlement_check.get("oidc_issuer_whitelist", [])
            ]
            if not (
                    int_data["authentication_method"] == "OIDC"
                    and normalize_issuer_url(int_data["oidc_issuer"]) in issuer_whitelist
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
            elif any(is_30day_trial(e) for e in eduperson_entitlements):
                # TODO: list multiple roles/levels? Better "status" signaling?
                user.info["roles"] = ["30DayTrial"]
                user.info["default_plan"] = self.BILLING_PLAN_30DAY_TRIAL
            else:
                _log.warning(f"user_access_validation failure: %r %r", enrollment_error_user_message, {
                    "user_id": user.user_id,
                    "eduperson_entitlements": eduperson_entitlements
                })
                raise PermissionsInsufficientException(enrollment_error_user_message)

        return user

    def health_check(self, options: Optional[dict] = None) -> Union[str, dict, flask.Response]:
        # TODO: use health check options?
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

    BILLING_PLAN_30DAY_TRIAL = "30day-trial"
    BILLING_PLAN_EARLY_ADOPTER = "early-adopter"

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
                    "name": self.BILLING_PLAN_30DAY_TRIAL,
                    "description": "openEO.cloud 30 day free trial plan (experimental)",
                    # TODO: url?
                    "paid": False
                },
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

    def service_types(self) -> dict:
        return self.secondary_services.service_types()

    def list_services(self, user_id: str) -> List[ServiceMetadata]:
        return self.secondary_services.list_services(user_id=user_id)

    def service_info(self, user_id: str, service_id: str) -> ServiceMetadata:
        return self.secondary_services.service_info(user_id=user_id, service_id=service_id)

    def create_service(self, user_id: str, process_graph: dict, service_type: str, api_version: str,
                       configuration: dict) -> str:
        return self.secondary_services.create_service(user_id=user_id, process_graph=process_graph,
            service_type=service_type, api_version=api_version, configuration=configuration)

    def remove_service(self, user_id: str, service_id: str) -> None:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/delete-service"""
        self.secondary_services.remove_service(user_id=user_id, service_id=service_id)

    def update_service(self, user_id: str, service_id: str, process_graph: dict) -> None:
        """https://openeo.org/documentation/1.0/developers/api/reference.html#operation/update-service"""
        self.secondary_services.update_service(
            user_id=user_id, service_id=service_id, process_graph=process_graph
        )