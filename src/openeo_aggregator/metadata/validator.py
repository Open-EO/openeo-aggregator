import inspect
import logging
from pathlib import Path

import argparse
import requests
from openeo_aggregator.config import get_config, AggregatorConfig
from openeo_aggregator.metadata.merging import (
    merge_collection_metadata,
    ProcessMetadataMerger,
)
from openeo_aggregator.metadata.reporter import MarkDownReporter

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
    target_group.add_argument(
        "-c", "--collections", action="store_true", help="Check collection metadata"
    )
    target_group.add_argument(
        "-p", "--processes", action="store_true", help="Check process metadata"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)
    _log.info(f"{args=}")

    # Load config
    config: AggregatorConfig = get_config(args.environment)

    # Determine backend ids/urls
    backend_ids = args.backends or list(config.aggregator_backends.keys())
    try:
        backend_urls = [config.aggregator_backends[b] for b in backend_ids]
    except KeyError:
        raise ValueError(
            f"Invalid backend ids {backend_ids}, should be subset of {list(config.aggregator_backends.keys())}."
        )
    print("\nRunning metadata merging checks against backend urls:")
    print("\n".join(f"- {bid}: {url}" for bid, url in zip(backend_ids, backend_urls)))
    print("\n")

    check_collections = args.collections
    check_processes = args.processes
    if not any([check_collections, check_processes]):
        check_collections = check_processes = True

    if check_collections:
        # Compare /collections
        compare_get_collections(backend_urls=backend_urls)
        # Compare /collections/{collection_id}
        for collection_id in get_all_collections_ids(backend_urls=backend_urls):
            compare_get_collection_by_id(
                backend_urls=backend_urls, collection_id=collection_id
            )
    if check_processes:
        # Compare /processes
        compare_get_processes(backend_urls=backend_urls)


def compare_get_collections(backend_urls):
    print("Comparing /collections")
    # Extract all collection objects from the backends
    backends_for_collection = {}
    for url in backend_urls:
        r = requests.get(url + "/collections")
        collections_result = r.json()
        for collection in collections_result["collections"]:
            if collection["id"] not in backends_for_collection:
                backends_for_collection[collection["id"]] = {}
            backends_for_collection[collection["id"]][url] = collection

    # Merge the different collection objects for each unique collection_id.
    reporter = MarkDownReporter()
    merged_metadata = {}
    for collection_id, backend_collection in backends_for_collection.items():
        by_backend = {}
        for url, collection in backend_collection.items():
            by_backend[url] = collection
        merged_metadata[collection_id] = merge_collection_metadata(
            by_backend, full_metadata=False, report=reporter.report
        )
    reporter.print()


def compare_get_collection_by_id(backend_urls, collection_id):
    print("Comparing /collections/{}".format(collection_id))
    by_backend = {}
    for url in backend_urls:
        r = requests.get(url + "/collections/{}".format(collection_id))
        if r.status_code == 200:
            by_backend[url] = r.json()
    reporter = MarkDownReporter()
    merged_metadata = merge_collection_metadata(
        by_backend, full_metadata=True, report=reporter.report
    )
    reporter.print()


def compare_get_processes(backend_urls):
    print("## Comparing /processes")
    processes_per_backend = {}
    for url in backend_urls:
        r = requests.get(url + "/processes")
        if r.status_code == 200:
            processes = r.json().get("processes", [])
            processes_per_backend[url] = {p["id"]: p for p in processes}
        else:
            print("WARNING: {} /processes does not return 200".format(url))
    reporter = MarkDownReporter()
    ProcessMetadataMerger(report=reporter.report).merge_processes_metadata(
        processes_per_backend
    )
    reporter.print()


def get_all_collections_ids(backend_urls):
    collection_ids = []
    for url in backend_urls:
        r = requests.get(url + "/collections")
        collections_result = r.json()
        collection_ids.extend([c["id"] for c in collections_result["collections"]])
    return collection_ids


if __name__ == "__main__":
    main()
