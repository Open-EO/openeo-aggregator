import argparse
import contextlib
import functools
import logging
from typing import List, Optional

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

import openeo_aggregator.caching
from openeo_aggregator.about import log_version_info
from openeo_aggregator.app import get_aggregator_logging_config
from openeo_aggregator.backend import AggregatorBackendImplementation
from openeo_aggregator.config import get_backend_config
from openeo_aggregator.connection import MultiBackendConnection

_log = logging.getLogger(__name__)


FAIL_MODE_FAILFAST = "failfast"
FAIL_MODE_WARN = "warn"


def main(args: Optional[List[str]] = None):
    """CLI entrypoint"""
    cli = argparse.ArgumentParser()
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
        require_zookeeper_writes=arguments.require_zookeeper_writes,
        fail_mode=arguments.fail_mode,
    )


def prime_caches(
    require_zookeeper_writes: bool = False,
    fail_mode: str = FAIL_MODE_FAILFAST,
):
    log_version_info(logger=_log)
    with TimingLogger(title=f"Prime caches", logger=_log):

        backends = MultiBackendConnection.from_config()
        backend_implementation = AggregatorBackendImplementation(backends=backends)

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
                    _log.debug(f"Get collection {c+1}/{len(collection_ids)} {collection_id}")
                    with fail_handler():
                        backend_implementation.catalog.get_collection_metadata(collection_id=collection_id)

        with TimingLogger(title="Get merged processes", logger=_log):
            with fail_handler():
                backend_implementation.processing.get_merged_process_metadata()

    if get_backend_config().zk_memoizer_tracking:
        kazoo_stats = openeo_aggregator.caching.zk_memoizer_stats
        zk_writes = sum(kazoo_stats.get(k, 0) for k in ["create", "set"])
        _log.info(f"ZooKeeper stats: {kazoo_stats=} {zk_writes=}")
        if require_zookeeper_writes and zk_writes == 0:
            raise RuntimeError("No ZooKeeper writes.")
    else:
        _log.warning(f"ZooKeeper stats: not configured")


if __name__ == "__main__":
    main()
