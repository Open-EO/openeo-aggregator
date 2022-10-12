import inspect
import logging
from pathlib import Path

import argparse
import requests
from openeo_aggregator.config import get_config, AggregatorConfig
from openeo_aggregator.metadata.merging import merge_collection_metadata

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
    print("Found backends:\n  * {}".format("\n  * ".join(urls)))

    # 1. Compare /collections
    compare_get_collections(urls)
    # 2. Compare /collections/{collection_id}
    # 3. Compare /processes
    # 4. Compare /processes/{process_id}


class ValidationReporter:
    def __init__(self):
        self.warning_messages = []
        self.critical_messages = []

    def report(self, msg, level="warning"):
        caller = inspect.stack()[1]
        caller_file = Path(caller.filename).name.split("/")[-1]
        msg = f"{caller_file}:{caller.lineno}: {msg}"
        self.critical_messages.append(msg) if level == "critical" else self.warning_messages.append(msg)

    def print(self):
        print("Warning messages:")
        for msg in self.warning_messages:
            print("  * {}".format(msg))
        print("Critical messages:")
        for msg in self.critical_messages:
            print("  * {}".format(msg))


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
        merged_metadata[collection_id] = merge_collection_metadata(by_backend, reporter.report)

    # Print the results
    reporter.print()


def compare_get_collection_by_id(backend_urls, collection_id):
    pass


def compare_get_processes(backend_urls):
    pass


def compare_get_process_by_id(backend_urls, process_id):
    pass


def get_all_collections_ids(backend_urls):
    collection_ids = []
    for url in backend_urls:
        r = requests.get(url + "/collections")
        collections_result = r.json()
        collection_ids.extend([c["id"] for c in collections_result["collections"]])
    return collection_ids


if __name__ == "__main__":
    main()
