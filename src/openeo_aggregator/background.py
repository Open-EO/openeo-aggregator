"""
Background logic to support the web app: cache maintenance, ...

"""
import logging
from pathlib import Path
from typing import Optional

from openeo.util import TimingLogger
from openeo_driver.util.logging import setup_logging

from openeo_aggregator.app import get_aggregator_logging_config
from openeo_aggregator.backend import AggregatorBackendImplementation
from openeo_aggregator.config import AggregatorConfig, get_config
from openeo_aggregator.connection import MultiBackendConnection

_log = logging.getLogger(__name__)


def prime_caches(config: Optional[str]):

    log = logging.getLogger(f"{__name__}.prime_caches")

    with TimingLogger(title="Prime caches", logger=log):

        config: AggregatorConfig = get_config(config)
        log.info(f"Using config: {config}")

        log.info(f"Creating MultiBackendConnection with {config.aggregator_backends}")
        backends = MultiBackendConnection.from_config(config)

        log.info("Creating AggregatorBackendImplementation")
        backend_implementation = AggregatorBackendImplementation(backends=backends, config=config)

        with TimingLogger(title="General capabilities", logger=log):
            backends.get_api_versions()
            backend_implementation.file_formats()
            backend_implementation.secondary_services.service_types()

        with TimingLogger(title="Get full collection listing", logger=log):
            collections_metadata = backend_implementation.catalog.get_all_metadata()

        with TimingLogger(title="Get per collection metadata", logger=log):
            collection_ids = [m["id"] for m in collections_metadata]
            for c, collection_id in enumerate(collection_ids):
                log.info(f"get collection {c+1}/{len(collection_ids)} {collection_id}")
                backend_implementation.catalog.get_collection_metadata(collection_id=collection_id)

        with TimingLogger(title="Get merged processes", logger=log):
            backend_implementation.processing.get_merged_process_metadata()


if __name__ == "__main__":
    setup_logging(
        config=get_aggregator_logging_config(
            context="background-task",
            handler_default_level="DEBUG",
        )
    )
    prime_caches(config=Path(__file__).parent.parent.parent / "conf/aggregator.dev.py")
