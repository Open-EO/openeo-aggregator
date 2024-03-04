import logging
import os
import re
from typing import Callable, Dict, List, Optional, Protocol, Union

import attrs
from openeo_driver.config import OpenEoBackendConfig
from openeo_driver.config.load import ConfigGetter
from openeo_driver.server import build_backend_deploy_metadata
from openeo_driver.utils import smart_bool

import openeo_aggregator.about

_log = logging.getLogger(__name__)


CACHE_TTL_DEFAULT = 6 * 60 * 60

# Timeouts for requests to back-ends
CONNECTION_TIMEOUT_DEFAULT = 60
CONNECTION_TIMEOUT_INIT = 12.5
CONNECTION_TIMEOUT_RESULT = 15 * 60
CONNECTION_TIMEOUT_JOB_START = 5 * 60
CONNECTION_TIMEOUT_JOB_LOGS = 2 * 60

STREAM_CHUNK_SIZE_DEFAULT = 10 * 1024


class ConfigException(ValueError):
    pass


class JobOptionsUpdater(Protocol):
    """API for `job_options_update` config (callable)"""

    def __call__(self, job_options: dict, backend_id: str) -> dict:
        """Return updated job options dict"""
        ...


@attrs.frozen(kw_only=True)
class AggregatorBackendConfig(OpenEoBackendConfig):

    capabilities_backend_version: str = openeo_aggregator.about.__version__
    capabilities_deploy_metadata: dict = build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo_aggregator"],
    )

    aggregator_backends: Dict[str, str] = attrs.field(validator=attrs.validators.min_len(1))

    # See `ZooKeeperPartitionedJobDB.from_config` for supported fields.
    partitioned_job_tracking: Optional[dict] = None

    streaming_chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT

    auth_entitlement_check: Union[bool, dict] = False

    # TTL for connection caching.
    connections_cache_ttl: float = 5 * 60.0

    # List of collection ids to cover with the aggregator (when None: support union of all upstream collections)
    collection_whitelist: Optional[List[Union[str, re.Pattern]]] = None

    zookeeper_prefix: str = "/openeo-aggregator/"

    # See `memoizer_from_config` for details.
    memoizer: Dict = attrs.Factory(lambda: {"type": "dict"})

    zk_memoizer_tracking: bool = smart_bool(os.environ.get("OPENEO_AGGREGATOR_ZK_MEMOIZER_TRACKING"))

    job_options_update: Optional[JobOptionsUpdater] = None


# Internal singleton
_config_getter = ConfigGetter(expected_class=AggregatorBackendConfig)


def get_backend_config() -> AggregatorBackendConfig:
    """Public config getter"""
    return _config_getter.get()
