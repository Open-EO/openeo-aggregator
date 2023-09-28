import argparse
import contextlib
import functools
import logging
from pathlib import Path
from typing import Any, List, Optional, Sequence, Union

from kazoo.client import KazooClient
from openeo.util import TimingLogger
from openeo_driver.util.logging import (
    LOG_HANDLER_FILE_JSON,
    LOG_HANDLER_ROTATING_FILE_JSON,
    LOG_HANDLER_STDERR_BASIC,
    LOG_HANDLER_STDERR_JSON,
    LOG_HANDLER_STDOUT_BASIC,
    LOG_HANDLER_STDOUT_JSON,
    just_log_exceptions,
    setup_logging,
)

from openeo_aggregator.app import get_aggregator_logging_config
from openeo_aggregator.backend import AggregatorBackendImplementation
from openeo_aggregator.config import (
    OPENEO_AGGREGATOR_CONFIG,
    AggregatorConfig,
    get_config,
)
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


FAIL_MODE_FAILFAST = "failfast"
FAIL_MODE_WARN = "warn"


def main(args: Optional[List[str]] = None):
    """CLI entrypoint"""
    cli = argparse.ArgumentParser()
    cli.add_argument(
        "--config",
        default=None,
        help=f"Optional: aggregator config to load (instead of env var {OPENEO_AGGREGATOR_CONFIG} based resolution).",
    )
    cli.add_argument(
        "--require-zookeeper-writes",
        action="store_true",
        help="Fail if no ZooKeeper writes were done",
    )
    cli.add_argument(
        "--fail-mode",
        choices=[FAIL_MODE_FAILFAST, FAIL_MODE_WARN],
        default=FAIL_MODE_FAILFAST,
        help="Fail mode: fail fast or warn (just log failures but keep going if possible).",
    )
    cli.add_argument(
        "--log-handler",
        dest="log_handlers",
        choices=[
            LOG_HANDLER_STDERR_BASIC,
            LOG_HANDLER_STDOUT_BASIC,
            LOG_HANDLER_STDERR_JSON,
            LOG_HANDLER_STDOUT_JSON,
            LOG_HANDLER_FILE_JSON,
            LOG_HANDLER_ROTATING_FILE_JSON,
        ],
        action="append",
        help="Log handlers.",
    )
    cli.add_argument(
        "--log-file",
        help="Log file to use for (rotating) file log handlers.",
    )

    arguments = cli.parse_args(args=args)

    logging_config = get_aggregator_logging_config(
        context="background-task", root_handlers=arguments.log_handlers, log_file=arguments.log_file
    )
    setup_logging(config=logging_config)
    prime_caches(
        config=arguments.config,
        require_zookeeper_writes=arguments.require_zookeeper_writes,
        fail_mode=arguments.fail_mode,
    )


def prime_caches(
    config: Union[str, Path, AggregatorConfig, None] = None,
    require_zookeeper_writes: bool = False,
    fail_mode: str = FAIL_MODE_FAILFAST,
):
    with TimingLogger(title="Prime caches", logger=_log):
        config: AggregatorConfig = get_config(config)
        _log.info(f"Using config: {config.get('config_source')=}")

        # Inject Zookeeper operation statistics
        kazoo_stats = {}
        _patch_config_for_kazoo_client_stats(config, kazoo_stats)

        _log.info(f"Creating AggregatorBackendImplementation with {config.aggregator_backends}")
        backends = MultiBackendConnection.from_config(config)
        backend_implementation = AggregatorBackendImplementation(backends=backends, config=config)

        if fail_mode == FAIL_MODE_FAILFAST:
            # Do not intercept any exceptions.
            fail_handler = contextlib.nullcontext
        elif fail_mode == FAIL_MODE_WARN:
            fail_handler = functools.partial(just_log_exceptions, log=_log)
        else:
            raise ValueError(fail_mode)

        with TimingLogger(title="General capabilities", logger=_log):
            with fail_handler():
                backends.get_api_versions()
            with fail_handler():
                backend_implementation.file_formats()
            with fail_handler():
                backend_implementation.secondary_services.service_types()

        with fail_handler():
            with TimingLogger(title="Get full collection listing", logger=_log):
                collections_metadata = backend_implementation.catalog.get_all_metadata()

            with TimingLogger(title="Get per collection metadata", logger=_log):
                collection_ids = [m["id"] for m in collections_metadata]
                for c, collection_id in enumerate(collection_ids):
                    _log.info(f"get collection {c+1}/{len(collection_ids)} {collection_id}")
                    with fail_handler():
                        backend_implementation.catalog.get_collection_metadata(collection_id=collection_id)

        with TimingLogger(title="Get merged processes", logger=_log):
            with fail_handler():
                backend_implementation.processing.get_merged_process_metadata()

    zk_writes = sum(kazoo_stats.get(k, 0) for k in ["create", "set"])
    _log.info(f"ZooKeeper stats: {kazoo_stats=} {zk_writes=}")
    if require_zookeeper_writes and zk_writes == 0:
        raise RuntimeError("No Zookeeper writes.")


def _patch_config_for_kazoo_client_stats(config: AggregatorConfig, stats: dict):
    orig_kazoo_client_factory = config.kazoo_client_factory or KazooClient
    def kazoo_client_factory(**kwargs):
        _log.info(f"AttrStatsProxy-wrapping KazooClient with {kwargs=}")
        zk = orig_kazoo_client_factory(**kwargs)
        return AttrStatsProxy(
            target=zk,
            to_track=["start", "stop", "create", "get", "set"],
            stats=stats,
        )

    _log.info(f"Patching config with {kazoo_client_factory=}")
    # TODO: create a new config instead of updating an existing one?
    config.kazoo_client_factory = kazoo_client_factory


if __name__ == "__main__":
    main()
