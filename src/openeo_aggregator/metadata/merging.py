"""

Functionality and tools for openEO collection/process metadata processing, normalizing, merging ...

"""
from collections import defaultdict

import flask
import itertools
import logging
from typing import Dict, Optional, Callable, Any, List

from openeo.util import rfc3339, dict_no_none
from openeo_aggregator.metadata import (
    STAC_PROPERTY_PROVIDER_BACKEND,
    FEDERATION_BACKENDS,
)
from openeo_aggregator.metadata.models.cube_dimensions import CubeDimensions
from openeo_aggregator.metadata.models.extent import Extent
from openeo_aggregator.metadata.models.stac_summaries import StacSummaries
from openeo_aggregator.metadata.reporter import LoggerReporter
from openeo_aggregator.utils import MultiDictGetter
from openeo_driver.errors import OpenEOApiException

_log = logging.getLogger(__name__)


DEFAULT_REPORTER = LoggerReporter(_log)


def normalize_collection_metadata(metadata: dict, app: Optional[flask.Flask] = None) -> dict:
    cid = metadata.get("id", None)
    if cid is None:
        raise OpenEOApiException("Missing collection id in metadata")
    if "links" not in metadata:
        metadata["links"] = []
    metadata["links"] = [l for l in metadata["links"] if l.get("rel") not in ("self", "parent", "root")]
    if app:
        metadata["links"].append({
            "href": app.url_for("openeo.collections", _external = True), "rel": "root"
        })
        metadata["links"].append({
            "href": app.url_for("openeo.collections", _external = True), "rel": "parent"
        })
        metadata["links"].append({
            "href": app.url_for("openeo.collection_by_id", collection_id = cid, _external = True), "rel": "self"
        })
    else:
        _log.warning("Unable to provide root/parent/self links in collection metadata outside flask app context")
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
    It takes in a message and level (e.g. "warning", "error") as arguments.
    """
    getter = MultiDictGetter(by_backend.values())

    cid = getter.single_value_for("id")
    _log.info(f"Merging collection metadata for {cid!r}")

    if full_metadata:
        for backend_id, collection in by_backend.items():
            for required_field in ["stac_version", "id", "description", "license", "extent", "links", "cube:dimensions",
                                   "summaries"]:
                if required_field not in collection:
                    report(
                        f"Missing {required_field} in collection metadata.",
                        collection_id=cid,
                        backend_id=backend_id,
                        level="error",
                    )

    # Start with some initial/required fields
    result = {
        "id": cid, "stac_version": max(list(getter.get("stac_version")) + ["0.9.0"]),
        "title": getter.first("title", default = cid), "description": getter.first("description", default = cid),
        "type": getter.first("type", default = "Collection"),
        "links": [k for k in getter.concat("links") # TODO: report invalid links (e.g. string instead of dict)
            if isinstance(k, dict) and k.get("rel") not in ("self", "parent", "root")],
    }

    # Generic field merging
    # Notes:
    # - `crs` is required by OGC API: https://docs.opengeospatial.org/is/18-058/18-058.html#_crs_identifier_list
    # - `sci:doi` and related are defined at https://github.com/stac-extensions/scientific
    for field in getter.available_keys(["stac_extensions", "keywords", "providers", "sci:publications"]):
        result[field] = getter.concat(field, skip_duplicates = True)
    for field in getter.available_keys(["deprecated"]):
        result[field] = all(getter.get(field))
    for field in getter.available_keys(["crs", "sci:citation", "sci:doi"]):
        result[field] = getter.first(field)

    # Summary merging
    summaries_list = []
    for i, cube_dim_dict in enumerate(getter.select("summaries").dictionaries):
        backend_id = list(by_backend.keys())[i]
        try:
            summary = StacSummaries.from_dict(cube_dim_dict)
            summaries_list.append((f"{backend_id}:{cid}", summary))
        except Exception as e:
            report(repr(e), collection_id=cid, backend_id=backend_id, level="warning")
    result["summaries"] = StacSummaries.merge_all(summaries_list, report).to_dict()

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
            report(repr(e), collection_id=cid, backend_id=backend_id, level="warning")
    result["extent"] = Extent.merge_all(extents).to_dict()

    if getter.has_key("cube:dimensions"):
        cube_dim_getter = getter.select("cube:dimensions")

        # First deserialize the cube:dimensions object to log any inconsistencies.
        for i, cube_dim_dict in enumerate(cube_dim_getter.dictionaries):
            backend_id = list(by_backend.keys())[i]
            try:
                CubeDimensions.from_dict(cube_dim_dict)
            except Exception as e:
                report(
                    repr(e), collection_id=cid, backend_id=backend_id, level="warning"
                )

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
                    level="warning",
                )
        # Temporal dimension
        t_dim = "t"
        if cube_dim_getter.has_key(t_dim):
            result["cube:dimensions"][t_dim] = cube_dim_getter.first(t_dim)
            # TODO: check consistency of step?
            t_extent = list(cube_dim_getter.select(t_dim).get("extent"))
            try:
                # TODO: Is multidict getter with id required?
                t_starts = [e[0] for e in t_extent if e[0] and e[0] != 'None']
                t_ends = [e[1] for e in t_extent if e[1] and e[1] != 'None']
                result["cube:dimensions"][t_dim]["extent"] = [
                    min(rfc3339.normalize(t) for t in t_starts) if t_starts else None,
                    max(rfc3339.normalize(t) for t in t_ends) if t_ends else None]
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
                bands_iterator = cube_dim_getter.select(dim).get("values")
                prefix = next(bands_iterator)
                for bands in bands_iterator:
                    prefix = [t[0] for t in itertools.takewhile(lambda t: t[0] == t[1], zip(prefix, bands))]
                    if bands != prefix:
                        report(
                            f"Trimming bands {bands} to common prefix {prefix}",
                            collection_id=cid,
                        )
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
    result["summaries"][STAC_PROPERTY_PROVIDER_BACKEND] = list(
        by_backend.keys()
    )  # TODO remove this deprecated field
    result["summaries"][FEDERATION_BACKENDS] = list(by_backend.keys())

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


def set_if_non_empty(d: dict, key: str, value: Any):
    """Helper to compactly set a key in a dictionary if the value is non-empty (aka "truthy")."""
    if value:
        d[key] = value


class ProcessMetadataMerger:
    def __init__(self, report: Callable = DEFAULT_REPORTER.report):
        self.report = report

    def merge_processes_metadata(
        self, processes_per_backend: Dict[str, Dict[str, dict]]
    ) -> Dict[str, dict]:
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
        for process_id, by_backend in grouped.items():
            try:
                merged[process_id] = self.merge_process_metadata(by_backend)
            except Exception as e:
                self.report(
                    f"Failed to merge process metadata: {e!r}", process_id=process_id
                )
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
        _log.info(f"Merging collection metadata for {process_id!r}")

        # Initialize
        merged = {
            # Some fields to always set (e.g. required)
            "id": process_id,
            "description": getter.first("description", default=process_id),
            FEDERATION_BACKENDS: supporting_backends,
        }
        set_if_non_empty(merged, "summary", getter.first("summary", default=None))

        merged["parameters"] = self._merge_process_parameters(
            by_backend=by_backend, process_id=process_id
        )

        # Return schema
        merged["returns"] = self._merge_process_returns(
            by_backend=by_backend, process_id=process_id
        )

        set_if_non_empty(
            merged, "exceptions", self._merge_process_exceptions(by_backend=by_backend)
        )
        set_if_non_empty(
            merged, "categories", self._merge_process_categories(by_backend=by_backend)
        )

        # TODO: merge "deprecated" field
        # TODO: merge "experimental" field
        # TODO: merge "examples" field
        # TODO: merge "links" field

        return merged

    def _get_parameters_by_name(
        self, parameters: List[dict], backend_id: str, process_id: str
    ) -> Dict[str, dict]:
        """Build dictionary of parameter metadata, keyed on name."""
        names = {}
        try:
            for param in parameters:
                try:
                    names[param["name"]] = param
                except Exception as e:
                    self.report(
                        f"Invalid parameter metadata {param!r}: {e!r}",
                        process_id=process_id,
                        backend_id=backend_id,
                    )
        except Exception as e:
            self.report(
                f"Invalid parameter listing {parameters!r}: {e!r}",
                backend_id=backend_id,
                process_id=process_id,
            )

        return names

    def _normalize_parameter(
        self,
        param: dict,
        strip_description: bool = False,
        add_optionals: bool = True,
    ) -> dict:
        """Normalize a parameter metadata dict"""
        # TODO: report missing name/description/schema?
        normalized = {
            "name": param.get("name", "n/a"),
            "schema": param.get("schema", {}),
        }
        if strip_description:
            normalized["description"] = "-"
        else:
            normalized["description"] = param.get("description", normalized["name"])
        for field, default_value in [
            ("optional", False),
            ("deprecated", False),
            ("experimental", False),
        ]:
            if add_optionals:
                normalized[field] = param.get(field, default_value)
            elif field in param:
                normalized[field] = param[field]
        # Required parameters SHOULD NOT specify a default value.
        # Optional parameters SHOULD always specify a default value.
        if normalized.get("optional", False):
            normalized["default"] = param.get("default", None)
        return normalized

    def _merge_process_parameters(
        self, by_backend: Dict[str, dict], process_id: str
    ) -> List[dict]:
        # Pick first non-empty parameter listing as merge result
        # TODO: real merge instead of taking first?
        merged = []
        merged_params_by_name = {}
        for backend_id, process_metadata in by_backend.items():
            params = process_metadata.get("parameters", [])
            if params:
                merged = [
                    self._normalize_parameter(
                        p, strip_description=False, add_optionals=False
                    )
                    for p in params
                ]
                merged_params_by_name = self._get_parameters_by_name(
                    parameters=merged, backend_id=backend_id, process_id=process_id
                )
                break

        # Check other parameter listings
        for backend_id, process_metadata in by_backend.items():
            params = process_metadata.get("parameters", [])
            params_by_name = self._get_parameters_by_name(
                parameters=params, backend_id=backend_id, process_id=process_id
            )
            missing_parameters = set(merged_params_by_name).difference(params_by_name)
            if missing_parameters:
                self.report(
                    f"Missing parameters: {missing_parameters!r}",
                    backend_id=backend_id,
                    process_id=process_id,
                )
            extra_parameters = set(params_by_name).difference(merged_params_by_name)
            if extra_parameters:
                self.report(
                    f"Extra parameters (not in merged listing): {extra_parameters}",
                    backend_id=backend_id,
                    process_id=process_id,
                )
            for name in set(merged_params_by_name).intersection(params_by_name):
                merged_param = self._normalize_parameter(
                    merged_params_by_name[name],
                    strip_description=True,
                    add_optionals=True,
                )
                other_param = self._normalize_parameter(
                    params_by_name[name], strip_description=True, add_optionals=True
                )
                for field in merged_param.keys():
                    merged_value = merged_param[field]
                    other_value = other_param[field]
                    if merged_value != other_value:
                        self.report(
                            f"Parameter {name!r} field {field!r} value {other_value!r} differs from merged {merged_value!r}",
                            backend_id=backend_id,
                            process_id=process_id,
                        )

        return merged

    def _merge_process_returns(
        self, by_backend: Dict[str, dict], process_id: str
    ) -> dict:
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
            # TODO: ignore description
            if other_returns != merged:
                self.report(
                    f"Returns schema {other_returns} is different from merged {merged}",
                    backend_id=backend_id,
                    process_id=process_id,
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
                    f"Invalid process exceptions listing: {exceptions!r}",
                    backend_id=backend_id,
                    process_id=process_id,
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
                    f"Invalid process categories listing: {categories!r}",
                    backend_id=backend_id,
                    process_id=process_id,
                )
        return sorted(merged)
