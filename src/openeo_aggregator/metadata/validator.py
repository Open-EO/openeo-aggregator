import argparse
import logging
from typing import Dict

import requests

from openeo_aggregator.config import get_backend_config
from openeo_aggregator.metadata.merging import (
    ProcessMetadataMerger,
    merge_collection_metadata,
)
from openeo_aggregator.metadata.reporter import MarkDownReporter
from openeo_aggregator.utils import MultiDictGetter

_log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--environment",
        help="Environment to load configuration for (e.g. 'dev', 'prod', 'local', ...)",
        default="dev",
    )
    parser.add_argument(
        "-b",
        "--backends",
        metavar="BACKEND_ID",
        nargs="+",
        help="List of backends to use (backend_id as used in configuration). Default: all backends from configuration",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose logging")
    target_group = parser.add_argument_group(
        "Target", "Which checks to run (if none specified, all checks will be run)."
    )
    target_group.add_argument("-c", "--collections", action="store_true", help="Check collection metadata")
    target_group.add_argument("-p", "--processes", action="store_true", help="Check process metadata")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)
    _log.info(f"{args=}")

    # Determine backend ids/urls
    aggregator_backends = get_backend_config().aggregator_backends
    backend_ids = args.backends or list(aggregator_backends.keys())
    try:
        backend_urls = [aggregator_backends[b] for b in backend_ids]
    except KeyError:
        raise ValueError(f"Invalid backend ids {backend_ids}, should be subset of {list(aggregator_backends.keys())}.")
    print("\nRunning metadata merging checks against backend urls:")
    print("\n".join(f"- {bid}: {url}" for bid, url in zip(backend_ids, backend_urls)))
    print("\n")

    check_collections = args.collections
    check_processes = args.processes
    if not any([check_collections, check_processes]):
        check_collections = check_processes = True

    if check_collections:
        backends_for_collection_id: Dict[str, Dict] = get_all_collection_ids(backend_urls)
        # Compare /collections
        compare_get_collections(backends_for_collection_id)
        # Compare /collections/{collection_id}
        for collection_id in sorted(set(backends_for_collection_id.keys())):
            compare_get_collection_by_id(backend_urls=backend_urls, collection_id=collection_id)
    if check_processes:
        # Compare /processes
        compare_get_processes(backend_urls=backend_urls)


def compare_get_collections(backends_for_collection_id):
    print("\n\n## Comparing `/collections`\n")
    # Merge the different collection objects for each unique collection_id.
    reporter = MarkDownReporter()
    for collection_id, by_backend in backends_for_collection_id.items():
        getter = MultiDictGetter(by_backend.values())
        cid = getter.single_value_for("id")
        if cid != collection_id:
            reporter.report("Collection id in metadata does not match id in url", collection_id=cid)
        merge_collection_metadata(by_backend, full_metadata=False, report=reporter.report)


def compare_get_collection_by_id(backend_urls, collection_id):
    # TODO: Only print header when there are issues to report?
    print(f"\n\n## Comparing `/collections/{collection_id}`\n")
    reporter = MarkDownReporter()
    by_backend = {}
    for url in backend_urls:
        # TODO: skip request if we know collection is not available on backend
        r = requests.get(url + "/collections/{}".format(collection_id))
        if r.status_code == 200:
            collection = r.json()
            if "id" in collection:
                by_backend[url] = collection
            else:
                reporter.report("Backend returned invalid collection", backend_id=url, collection_id=collection_id)
    merge_collection_metadata(by_backend, full_metadata=True, report=reporter.report)


def compare_get_processes(backend_urls):
    print("\n\n## Comparing /processes\n")
    processes_per_backend = {}
    for url in backend_urls:
        r = requests.get(url + "/processes")
        r.raise_for_status()
        processes = r.json().get("processes", [])
        processes_per_backend[url] = {p["id"]: p for p in processes}
    reporter = MarkDownReporter()
    ProcessMetadataMerger(report=reporter.report).merge_processes_metadata(processes_per_backend)


def get_all_collection_ids(backend_urls) -> Dict[str, Dict[str, Dict]]:
    """
    Args:
        backend_urls: The backends to retrieve the set of collections from.

    Returns:
        Returns a dictionary mapping each collection id to a dictionary of {backend_url: collection_metadata}
        key-value pairs.
    """
    reporter = MarkDownReporter()
    backends_for_collection_id = {}
    for url in backend_urls:
        r = requests.get(url + "/collections")
        r.raise_for_status()
        collections_result = r.json()
        for collection in collections_result["collections"]:
            cid = collection["id"]
            if cid == "":
                reporter.report(f"Backend contains collection with empty string as id", backend_id=url)
                continue
            if cid not in backends_for_collection_id:
                backends_for_collection_id[cid] = {}
            backends_for_collection_id[cid][url] = collection
    return backends_for_collection_id


if __name__ == "__main__":
    main()
