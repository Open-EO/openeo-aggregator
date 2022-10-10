"""

Functionality and tools for openEO collection/process metadata processing, normalizing, merging ...

"""
import itertools
import logging
from typing import Dict, Optional, Callable

import flask

from openeo.util import rfc3339
from openeo_aggregator.metadata import STAC_PROPERTY_PROVIDER_BACKEND
from openeo_aggregator.utils import MultiDictGetter
from openeo_driver.errors import OpenEOApiException

_log = logging.getLogger(__name__)


def normalize_collection_metadata(metadata: dict, app: Optional[flask.Flask] = None) -> dict:
    cid = metadata.get("id", None)
    if cid is None:
        raise OpenEOApiException("Missing collection id in metadata")
    if "links" not in metadata:
        metadata["links"] = []
    metadata["links"] = [l for l in metadata["links"] if l.get("rel") not in ("self", "parent", "root")]
    if app:
        metadata["links"].append({
            "href": app.url_for("openeo.collections", _external=True),
            "rel": "root"
        })
        metadata["links"].append({
            "href": app.url_for("openeo.collections", _external=True),
            "rel": "parent"
        })
        metadata["links"].append({
            "href": app.url_for("openeo.collection_by_id", collection_id=cid, _external=True),
            "rel": "self"
        })
    else:
        _log.warning("Unable to provide root/parent/self links in collection metadata outside flask app context")
    return metadata


def merge_collection_metadata(by_backend: Dict[str, dict], report: Callable[[str, str], None]):
    """
    Merge collection metadata dicts from multiple backends

    :param by_backend: mapping of backend id to collection metadata dict
    :param report: function to report issues in the merging process
    It takes in a message and level (e.g. "warning", "error") as arguments.
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
    result["summaries"][STAC_PROPERTY_PROVIDER_BACKEND] = list(by_backend.keys())

    ## Log warnings for improper metadata.
    # license => Log warning for collections without license links.
    # TODO: report invalid links
    license_links = [k for k in getter.concat("links") if isinstance(k, dict) and k.get("rel") == "license"]
    if result["license"] in ["various", "proprietary"] and not license_links:
        _log.warning(f"Missing license links for collection: {cid}")

    return result
