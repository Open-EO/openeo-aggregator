import logging
from pathlib import Path
from typing import Any, Optional, Sequence, Union

from kazoo.client import KazooClient
from openeo.util import TimingLogger
from openeo_driver.util.logging import setup_logging

from openeo_aggregator.app import get_aggregator_logging_config
from openeo_aggregator.backend import AggregatorBackendImplementation
from openeo_aggregator.config import AggregatorConfig, get_config
from openeo_aggregator.connection import MultiBackendConnection

_log = logging.getLogger(__name__)


class AttrStatsProxy:
    """
    Proxy object to wrap a given object and keep stats of attribute/method usage.
    """

    # TODO: move this to a utilities module
    # TODO: avoid all these public attributes that could collide with existing attributes of the proxied object
    __slots__ = ["target", "to_track", "stats"]

    def __init__(self, target: Any, to_track: Sequence[str], stats: Optional[dict] = None):
        self.target = target
        self.to_track = set(to_track)
        self.stats = stats if stats is not None else {}

    def __getattr__(self, name):
        if name in self.to_track:
            self.stats[name] = self.stats.get(name, 0) + 1
        return getattr(self.target, name)


def main(config: Union[str, Path, AggregatorConfig, None] = None):
    """CLI entrypoint"""
    setup_logging(config=get_aggregator_logging_config(context="background-task"))
    prime_caches(config=config)


def prime_caches(config: Union[str, Path, AggregatorConfig, None] = None):
    with TimingLogger(title="Prime caches", logger=_log):
        config: AggregatorConfig = get_config(config)
        _log.info(f"Using config: {config.get('config_source')=}")

        # Inject Zookeeper operation statistics
        kazoo_stats = {}
        _patch_config_for_kazoo_client_stats(config, kazoo_stats)

        _log.info(f"Creating AggregatorBackendImplementation with {config.aggregator_backends}")
        backends = MultiBackendConnection.from_config(config)
        backend_implementation = AggregatorBackendImplementation(backends=backends, config=config)

        with TimingLogger(title="General capabilities", logger=_log):
            backends.get_api_versions()
            backend_implementation.file_formats()
            backend_implementation.secondary_services.service_types()

        with TimingLogger(title="Get full collection listing", logger=_log):
            collections_metadata = backend_implementation.catalog.get_all_metadata()

        with TimingLogger(title="Get per collection metadata", logger=_log):
            collection_ids = [m["id"] for m in collections_metadata]
            for c, collection_id in enumerate(collection_ids):
                _log.info(f"get collection {c+1}/{len(collection_ids)} {collection_id}")
                backend_implementation.catalog.get_collection_metadata(collection_id=collection_id)

        with TimingLogger(title="Get merged processes", logger=_log):
            backend_implementation.processing.get_merged_process_metadata()

    _log.info(f"Zookeeper stats: {kazoo_stats}")


def _patch_config_for_kazoo_client_stats(config: AggregatorConfig, stats: dict):
    def kazoo_client_factory(**kwargs):
        _log.info(f"AttrStatsProxy-wrapping KazooClient with {kwargs=}")
        zk = KazooClient(**kwargs)
        return AttrStatsProxy(
            target=zk,
            to_track=["start", "stop", "create", "get", "set"],
            stats=stats,
        )

    _log.info(f"Patching config with {kazoo_client_factory=}")
    # TODO: create a new config instead of updating an existing one?
    config.kazoo_client_factory = kazoo_client_factory


if __name__ == "__main__":
    main(config=Path(__file__).parent.parent.parent.parent / "conf/aggregator.dev.py")
