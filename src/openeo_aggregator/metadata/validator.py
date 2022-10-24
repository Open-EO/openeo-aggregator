import inspect
import logging
from pathlib import Path

import argparse
import requests
from openeo_aggregator.config import get_config, AggregatorConfig
from openeo_aggregator.metadata.merging import merge_collection_metadata, merge_process_metadata
from openeo_aggregator.metadata.reporter import ValidationReporter

_log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--backends", nargs="+", help="List of backends to use", )
    parser.add_argument("-e", "--environment", help="Environment to use", default="dev")
    args = parser.parse_args()
    print("Requested backends: {}".format(args.backends))
    print("Requested environment: {}".format(args.environment))
    config: AggregatorConfig = get_config(args.environment)
    backends = args.backends
    if not backends or len(backends) == 0:
        backends = config.aggregator_backends.keys()

    urls = [url for b, url in config.aggregator_backends.items() if b in backends]
    print("Using backends:\n  * {}".format("\n  * ".join(urls)))

    # 1. Compare /collections
    compare_get_collections(urls)
    # 2. Compare /collections/{collection_id}
    for collection_id in get_all_collections_ids(urls):
        compare_get_collection_by_id(urls, collection_id)
    # 3. Compare /processes
    compare_get_processes(urls)


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
    reporter = ValidationReporter()
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
    reporter = ValidationReporter()
    merged_metadata = merge_collection_metadata(
        by_backend, full_metadata=True, report=reporter.report
    )
    reporter.print()


def compare_get_processes(backend_urls):
    print("Comparing /processes")
    processes_per_backend = {}
    for url in backend_urls:
        r = requests.get(url + "/processes")
        if r.status_code == 200:
            processes = r.json().get("processes", {})
            processes_per_backend[url] = {p["id"]: p for p in processes}
        else:
            print("WARNING: {} /processes does not return 200".format(url))
    reporter = ValidationReporter()
    merge_process_metadata(processes_per_backend, reporter.report)
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
