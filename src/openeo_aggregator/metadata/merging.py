"""

Functionality and tools for openEO collection/process metadata processing, normalizing, merging ...

"""

import difflib
import functools
import json
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

import flask
from openeo.util import deep_get, rfc3339
from openeo_driver.errors import OpenEOApiException

from openeo_aggregator.metadata import (
    STAC_PROPERTY_FEDERATION_BACKENDS,
    STAC_PROPERTY_PROVIDER_BACKEND,
)
from openeo_aggregator.metadata.models.cube_dimensions import CubeDimensions
from openeo_aggregator.metadata.models.extent import Extent
from openeo_aggregator.metadata.models.stac_summaries import StacSummaries
from openeo_aggregator.metadata.reporter import LoggerReporter
from openeo_aggregator.utils import MultiDictGetter, common_prefix, drop_dict_keys

_log = logging.getLogger(__name__)


DEFAULT_REPORTER = LoggerReporter(_log)


def normalize_collection_metadata(metadata: dict) -> dict:
    cid = metadata.get("id", None)
    if cid is None:
        raise OpenEOApiException("Missing collection id in metadata")
    # Remove the root/parent/self links, so that aggregator-specific ones
    # are automatically added by `_normalize_collection_metadata` from openeo-python-driver (0.67.0, 133e6aa5)
    if "links" in metadata:
        links_to_keep = [ln for ln in metadata.pop("links") if ln.get("rel") not in ("self", "parent", "root")]
        if links_to_keep:
            metadata["links"] = links_to_keep

    return metadata


def merge_collection_metadata(
    by_backend: Dict[str, dict],
    full_metadata: bool,
    report: Callable = DEFAULT_REPORTER.report,
) -> dict:
    """
    Merge collection metadata dicts (about same or "to be unified" collection) from multiple backends

    :param by_backend: mapping of backend id to collection metadata dict
    :param full_metadata: indicates whether to work with full collection metadata (instead of basic).
    :param report: function to report issues in the merging process
    """
    getter = MultiDictGetter(by_backend.values())

    cid = getter.single_value_for("id")
    _log.info(f"Merging collection metadata for {cid!r}")

    if full_metadata:
        for backend_id, collection in by_backend.items():
            for required_field in [
                "stac_version",
                "id",
                "description",
                "license",
                "extent",
                "links",
                "cube:dimensions",
                "summaries",
            ]:
                if required_field not in collection:
                    report(
                        f"Missing {required_field} in collection metadata.",
                        collection_id=cid,
                        backend_id=backend_id,
                    )

    # Start with some initial/required fields
    result = {
        "id": cid,
        "stac_version": max(list(getter.get("stac_version")) + ["0.9.0"]),
        "title": getter.first("title", default=cid),
        "description": getter.first("description", default=cid),
        "type": getter.first("type", default="Collection"),
        "links": [
            k
            for k in getter.concat(
                "links", skip_duplicates=True
            )  # TODO: report invalid links (e.g. string instead of dict)
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
    summaries_by_backend = {}
    for backend_id, collection in by_backend.items():
        try:
            if "summaries" in collection:
                summaries_by_backend[backend_id] = StacSummaries.from_dict(collection.get("summaries"))
        except Exception as e:
            report("Failed to parse summaries", collection_id=cid, backend_id=backend_id, exception=e)
    result["summaries"] = StacSummaries.merge_all(
        summaries_by_backend=summaries_by_backend, report=report, collection_id=cid
    ).to_dict()

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

    extents = []
    for i, extent_dict in enumerate(getter.select("extent").dictionaries):
        backend_id = list(by_backend.keys())[i]
        try:
            extent = Extent.from_dict(extent_dict)
            extents.append((f"{backend_id}:{cid}", extent))
        except Exception as e:
            report(repr(e), collection_id=cid, backend_id=backend_id)
    result["extent"] = Extent.merge_all(extents).to_dict()

    if getter.has_key("cube:dimensions"):
        cube_dim_getter = getter.select("cube:dimensions")

        # First deserialize the cube:dimensions object to log any inconsistencies.
        for i, cube_dim_dict in enumerate(cube_dim_getter.dictionaries):
            backend_id = list(by_backend.keys())[i]
            try:
                CubeDimensions.from_dict(cube_dim_dict)
            except Exception as e:
                report(repr(e), collection_id=cid, backend_id=backend_id)

        # Then merge the cube:dimensions objects into one.
        result["cube:dimensions"] = {}
        # Spatial dimensions
        for dim in cube_dim_getter.available_keys(["x", "y"]):
            result["cube:dimensions"][dim] = cube_dim_getter.first(dim)
            # TODO: check consistency of step and reference_system?
            try:
                bounds = cube_dim_getter.select(dim).concat("extent")
                result["cube:dimensions"][dim]["extent"] = [min(bounds), max(bounds)]
            except Exception as e:
                report(
                    f"Failed to merge cube:dimensions.{dim}.extent: {e!r}",
                    collection_id=cid,
                )
        # Temporal dimension
        t_dim = "t"
        if cube_dim_getter.has_key(t_dim):
            result["cube:dimensions"][t_dim] = cube_dim_getter.first(t_dim)
            # TODO: check consistency of step?
            t_extent = list(cube_dim_getter.select(t_dim).get("extent"))
            try:
                # TODO: Is multidict getter with id required?
                t_starts = [e[0] for e in t_extent if e[0] and e[0] != "None"]
                t_ends = [e[1] for e in t_extent if e[1] and e[1] != "None"]
                result["cube:dimensions"][t_dim]["extent"] = [
                    min(rfc3339.normalize(t) for t in t_starts) if t_starts else None,
                    max(rfc3339.normalize(t) for t in t_ends) if t_ends else None,
                ]
            except Exception as e:
                report(
                    f"Failed to merge cube:dimensions.{t_dim}.extent: {e!r}, actual: {t_extent}",
                    collection_id=cid,
                )

        for dim in cube_dim_getter.available_keys(["bands"]):
            result["cube:dimensions"][dim] = cube_dim_getter.first(dim)
            try:
                # Find common prefix of bands
                # TODO: better approach? e.g. keep everything and rewrite process graphs on the fly?
                prefix = common_prefix(cube_dim_getter.select(dim).get("values"))
                if len(prefix) > 0:
                    result["cube:dimensions"][dim]["values"] = prefix
                else:
                    report(
                        f"Empty prefix for bands, falling back to first back-end's bands",
                        collection_id=cid,
                    )
            except Exception as e:
                report(
                    f"Failed to merge cube:dimensions.{dim}.extent: {e!r}",
                    collection_id=cid,
                )

    # TODO: use a more robust/user friendly backend pointer than backend id (which is internal implementation detail)
    result["summaries"][STAC_PROPERTY_PROVIDER_BACKEND] = list(by_backend.keys())
    result["summaries"][STAC_PROPERTY_FEDERATION_BACKENDS] = list(by_backend.keys())

    ## Log warnings for improper metadata.
    # license => Log warning for collections without license links.
    # TODO: report invalid links
    license_links = [k for k in getter.concat("links") if isinstance(k, dict) and k.get("rel") == "license"]
    if result["license"] in ["various", "proprietary"] and not license_links:
        lc = result["license"]
        license_links_str = ", ".join(license_links)
        report(
            f"License is '{lc}' but can not be found in license_links {license_links_str}",
            collection_id=cid,
        )
    return result


def single_backend_collection_post_processing(metadata: dict, *, backend_id: str):
    """In-place post-processing of a single backend collection"""
    if not deep_get(metadata, "summaries", STAC_PROPERTY_FEDERATION_BACKENDS, default=None):
        metadata.setdefault("summaries", {})
        metadata["summaries"][STAC_PROPERTY_FEDERATION_BACKENDS] = [backend_id]
    else:
        _log.warning(
            f"Summary {STAC_PROPERTY_FEDERATION_BACKENDS} is already set on collection {metadata.get('id', 'n/a')}, which is weird."
        )


def set_if_non_empty(d: dict, key: str, value: Any):
    """Helper to compactly set a key in a dictionary if the value is non-empty (aka "truthy")."""
    if value:
        d[key] = value


def json_diff(a: Any, b: Any, a_name: str = "", b_name: str = "", context: int = 3) -> List[str]:
    """
    Generate unified diff of JSON serialization of given objects
    :return: List of diff lines
    """

    def sort_dicts(x: Any) -> Any:
        """Recursively sort dictionaries in nested data structure"""
        if isinstance(x, dict):
            return {k: sort_dicts(v) for (k, v) in sorted(x.items())}
        elif isinstance(x, (list, tuple)):
            return type(x)(sort_dicts(v) for v in x)
        else:
            return x

    a_json = json.dumps(sort_dicts(a), indent=2) + "\n"
    b_json = json.dumps(sort_dicts(b), indent=2) + "\n"
    return list(
        difflib.unified_diff(
            a_json.splitlines(keepends=True),
            b_json.splitlines(keepends=True),
            fromfile=a_name,
            tofile=b_name,
            n=context,
        )
    )


def ignore_description(data: Any) -> Any:
    return drop_dict_keys(data, keys=["description"])


class ProcessMetadataMerger:
    def __init__(self, report: Callable = DEFAULT_REPORTER.report):
        self.report = report

    def merge_processes_metadata(self, processes_per_backend: Dict[str, Dict[str, dict]]) -> Dict[str, dict]:
        """
        Merge process metadata listings from multiple back-ends into a single process listing.

        :param processes_per_backend: A dictionary mapping backend ids to processes.
        :return: Mapping of process id to process metadata dict
        """
        # Regroup mapping from backend_id -> process_id -> metadata
        # to process_id -> backend_id -> metadata
        grouped: Dict[str, Dict[str, dict]] = defaultdict(dict)
        for backend_id, backend_processes in processes_per_backend.items():
            for process_id, process_metadata in backend_processes.items():
                grouped[process_id][backend_id] = process_metadata

        merged: Dict[str, dict] = {}
        for process_id, by_backend in sorted(grouped.items()):
            try:
                merged[process_id] = self.merge_process_metadata(by_backend)
            except Exception as e:
                self.report(f"Failed to merge process metadata: {e!r}", process_id=process_id)
        return merged

    def merge_process_metadata(self, by_backend: Dict[str, dict]) -> dict:
        """
        Merge process metadata of same process across multiple back-ends
        into a single process metadata dict.

        :param by_backend: A dictionary mapping backend ids to process metadata.
        :return: A single process metadata dict.
        """
        supporting_backends = list(by_backend.keys())
        getter = MultiDictGetter(by_backend.values())

        process_id = getter.single_value_for("id")
        _log.info(f"Merging process metadata for {process_id!r}")

        # Initialize
        merged = {
            # Some fields to always set (e.g. required)
            "id": process_id,
            "description": getter.first("description", default=process_id),
            STAC_PROPERTY_FEDERATION_BACKENDS: supporting_backends,
        }
        set_if_non_empty(merged, "summary", getter.first("summary", default=None))

        merged["parameters"] = self._merge_process_parameters(by_backend=by_backend, process_id=process_id)

        # Return schema
        merged["returns"] = self._merge_process_returns(by_backend=by_backend, process_id=process_id)

        set_if_non_empty(merged, "exceptions", self._merge_process_exceptions(by_backend=by_backend))
        set_if_non_empty(merged, "categories", self._merge_process_categories(by_backend=by_backend))

        merged["deprecated"] = any(getter.get("deprecated"))
        merged["experimental"] = any(getter.get("experimental"))
        merged["examples"] = getter.concat("examples", skip_duplicates=True)
        merged["links"] = getter.concat("links", skip_duplicates=True)

        return merged

    def _get_parameters_by_name(self, parameters: List[dict], backend_id: str, process_id: str) -> Dict[str, dict]:
        """Build dictionary of parameter metadata, keyed on name."""
        names = {}
        try:
            for param in parameters:
                try:
                    names[param["name"]] = param
                except Exception as e:
                    self.report(
                        "Invalid parameter metadata",
                        process_id=process_id,
                        backend_id=backend_id,
                        parameter=param,
                        exception=e,
                    )
        except Exception as e:
            self.report(
                "Invalid parameter listing",
                backend_id=backend_id,
                process_id=process_id,
                parameters=parameters,
                exception=e,
            )

        return names

    def _merge_process_parameters(self, by_backend: Dict[str, dict], process_id: str) -> List[dict]:
        # Pick first non-empty parameter listing as merge result
        # TODO: real merge instead of taking first?
        merged = []
        merged_params_by_name = {}
        for backend_id, process_metadata in by_backend.items():
            params = process_metadata.get("parameters", [])
            if params:
                normalizer = ProcessParameterNormalizer(
                    strip_description=False,
                    add_optionals=False,
                    report=functools.partial(self.report, backend_id=backend_id, process_id=process_id),
                )
                merged = normalizer.normalize_parameters(params)

                merged_params_by_name = self._get_parameters_by_name(
                    parameters=merged, backend_id=backend_id, process_id=process_id
                )
                break

        # Check other parameter listings against merged
        for backend_id, process_metadata in by_backend.items():
            params = process_metadata.get("parameters", [])
            params_by_name = self._get_parameters_by_name(
                parameters=params, backend_id=backend_id, process_id=process_id
            )
            missing_parameters = set(merged_params_by_name).difference(params_by_name)
            if missing_parameters:
                self.report(
                    "Missing parameters.",
                    backend_id=backend_id,
                    process_id=process_id,
                    missing_parameters=sorted(missing_parameters),
                )
            extra_parameters = set(params_by_name).difference(merged_params_by_name)
            if extra_parameters:
                self.report(
                    "Extra parameters (not in merged listing).",
                    backend_id=backend_id,
                    process_id=process_id,
                    extra_parameters=sorted(extra_parameters),
                )
            for name in sorted(set(merged_params_by_name).intersection(params_by_name)):
                normalizer = ProcessParameterNormalizer(
                    strip_description=True,
                    add_optionals=True,
                    report=functools.partial(self.report, backend_id=backend_id, process_id=process_id),
                )
                merged_param = normalizer.normalize_parameter(merged_params_by_name[name])
                other_param = normalizer.normalize_parameter(params_by_name[name])
                for field in merged_param.keys():
                    merged_value = merged_param[field]
                    other_value = other_param[field]
                    if merged_value != other_value:
                        self.report(
                            f"Parameter {name!r} field {field!r} value differs from merged.",
                            backend_id=backend_id,
                            process_id=process_id,
                            merged=merged_value,
                            value=other_value,
                            diff=json_diff(
                                a=merged_value,
                                b=other_value,
                                a_name="merged",
                                b_name=backend_id,
                            ),
                        )

        return merged

    def _merge_process_returns(self, by_backend: Dict[str, dict], process_id: str) -> dict:
        """
        Merge `returns` metadata
        :param by_backend: {backend_id: process_metadata}
        :param process_id:
        :return:
        """
        getter = MultiDictGetter(by_backend.values())
        # TODO: real merge instead of taking first schema as "merged" schema?
        merged = getter.first("returns", {"schema": {}})

        for backend_id, process_metadata in by_backend.items():
            other_returns = process_metadata.get("returns", {"schema": {}})
            if ignore_description(other_returns) != ignore_description(merged):
                self.report(
                    f"Returns schema is different from merged.",
                    backend_id=backend_id,
                    process_id=process_id,
                    merged=merged,
                    value=other_returns,
                    diff=json_diff(merged, other_returns, a_name="merged", b_name=backend_id),
                )
        return merged

    def _merge_process_exceptions(self, by_backend: Dict[str, dict]):
        merged = {}
        for backend_id, process_metadata in by_backend.items():
            process_id = process_metadata.get("id", "n/a")
            exceptions = process_metadata.get("exceptions", {})
            if isinstance(exceptions, dict):
                # TODO: take value from first backend instead of last one here?
                merged.update(exceptions)
            else:
                self.report(
                    f"Invalid process exceptions listing",
                    backend_id=backend_id,
                    process_id=process_id,
                    exceptions=exceptions,
                )
        return merged

    def _merge_process_categories(self, by_backend: Dict[str, dict]):
        merged = set()
        for backend_id, process_metadata in by_backend.items():
            process_id = process_metadata.get("id", "n/a")
            categories = process_metadata.get("categories", [])
            if isinstance(categories, list):
                merged.update(categories)
            else:
                self.report(
                    f"Invalid process categories listing.",
                    backend_id=backend_id,
                    process_id=process_id,
                    categories=categories,
                )
        return sorted(merged)


class ProcessParameterNormalizer:
    """
    Helper class to normalize process parameters
    (set default values, strip descriptions),
    e.g. for comparison purposes.
    """

    __slots__ = ["strip_description", "add_optionals", "report"]

    def __init__(
        self,
        strip_description: bool = False,
        add_optionals: bool = True,
        report: Callable = DEFAULT_REPORTER.report,
    ):
        self.strip_description = strip_description
        self.add_optionals = add_optionals
        self.report = report

    def normalize_parameter(self, param: dict) -> dict:
        """Normalize a parameter metadata dict"""
        for required in ["name", "schema", "description"]:
            if required not in param:
                self.report(f"Missing required field {required!r} in parameter metadata {param!r}")
        normalized = {
            "name": param.get("name", "n/a"),
            "schema": param.get("schema", {}),
        }
        if self.strip_description:
            normalized["description"] = "-"
        else:
            normalized["description"] = param.get("description", normalized["name"])
        for field, default_value in [
            ("optional", False),
            ("deprecated", False),
            ("experimental", False),
        ]:
            if self.add_optionals:
                normalized[field] = param.get(field, default_value)
            elif field in param:
                normalized[field] = param[field]
        # Required parameters SHOULD NOT specify a default value.
        # Optional parameters SHOULD always specify a default value.
        if normalized.get("optional", False):
            normalized["default"] = param.get("default", None)

        # Recurse into sub-process graphs under "schema" to normalize nested parameters
        normalized["schema"] = self.normalize_recursively(normalized["schema"])

        return normalized

    def normalize_parameters(self, parameters: List[dict]) -> List[dict]:
        """Normalize a list of parameter dicts."""
        return [self.normalize_parameter(param) for param in parameters]

    def normalize_recursively(self, x: Any) -> Any:
        """
        Recursively walk dictionary and lists and normalize process parameters along the way.
        """
        if isinstance(x, dict):
            if (
                x.get("type") == "object"
                and x.get("subtype") == "process-graph"
                and isinstance(x.get("parameters"), list)
            ):
                return {k: self.normalize_parameters(parameters=v) if k == "parameters" else v for k, v in x.items()}
            else:
                return {k: self.normalize_recursively(v) for k, v in x.items()}
        elif isinstance(x, list):
            return [self.normalize_recursively(v) for v in x]
        else:
            return x
