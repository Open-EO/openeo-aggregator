import logging
import os
import re
from pathlib import Path
from typing import Callable, List, Optional, Union

import attrs
from openeo_driver.config import OpenEoBackendConfig
from openeo_driver.config.load import ConfigGetter
from openeo_driver.server import build_backend_deploy_metadata
from openeo_driver.users.oidc import OidcProvider
from openeo_driver.utils import dict_item

import openeo_aggregator.about

_log = logging.getLogger(__name__)

OPENEO_AGGREGATOR_CONFIG = "OPENEO_AGGREGATOR_CONFIG"

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


class AggregatorConfig(dict):
    """
    Simple dictionary based configuration for aggregator backend
    """

    # TODO #112 migrate everything to AggregatorBackendConfig (attrs based instead of dictionary based)

    config_source = dict_item()

    # Dictionary mapping backend id to backend url
    aggregator_backends = dict_item()

    # TODO #112 `configured_oidc_providers` is deprecated, use `OpenEoBackendConfig.oidc_providers` instead
    configured_oidc_providers: List[OidcProvider] = dict_item(default=[])

    partitioned_job_tracking = dict_item(default=None)
    zookeeper_prefix = dict_item(default="/openeo-aggregator/")
    kazoo_client_factory = dict_item(default=None)

    # See `memoizer_from_config` for details.
    memoizer = dict_item(default={"type": "dict"})

    # Just a config field for test purposes (while were stripping down this config class)
    test_dummy = dict_item(default="alice")

    @staticmethod
    def from_py_file(path: Union[str, Path]) -> 'AggregatorConfig':
        """Load config from Python file."""
        path = Path(path)
        _log.debug(f"Loading config from Python file {path}")
        # Based on flask's Config.from_pyfile
        with path.open(mode="rb") as f:
            code = compile(f.read(), path, "exec")
        globals = {"__file__": str(path)}
        exec(code, globals)
        for var_name in ["aggregator_config", "config"]:
            if var_name in globals:
                config = globals[var_name]
                _log.info(f"Loaded config from {path=} {var_name=}")
                break
        else:
            raise ConfigException(f"No 'config' variable defined in config file {path}")
        if not isinstance(config, AggregatorConfig):
            raise ConfigException(f"Variable 'config' from {path} is not AggregatorConfig but {type(config)}")
        return config

    def copy(self) -> 'AggregatorConfig':
        return AggregatorConfig(self)


def get_config_dir() -> Path:
    # TODO: make this robust against packaging operations (e.g. no guaranteed real __file__ path)
    # TODO #117: eliminate the need for this hardcoded, package based config dir
    for root in [Path.cwd(), Path(__file__).parent.parent.parent]:
        config_dir = root / "conf"
        if config_dir.is_dir():
            return config_dir
    raise RuntimeError("No config dir found")


def get_config(x: Union[str, Path, AggregatorConfig, None] = None) -> AggregatorConfig:
    """
    Get aggregator config from given object:
    - if None: check env variable "OPENEO_AGGREGATOR_CONFIG" or return default config
    - if it is already an `AggregatorConfig` object: return as is
    - if it is a string and looks like a path of a Python file: load config from that
    """

    if x is None:
        if OPENEO_AGGREGATOR_CONFIG in os.environ:
            x = os.environ[OPENEO_AGGREGATOR_CONFIG]
            _log.info(f"Config from env var {OPENEO_AGGREGATOR_CONFIG}: {x!r}")
        else:
            x = get_config_dir() / "aggregator.dummy.py"
            _log.info(f"Config from fallback {x!r}")

    if isinstance(x, str):
        x = Path(x)

    if isinstance(x, AggregatorConfig):
        return x
    elif isinstance(x, Path) and x.suffix.lower() == ".py":
        return AggregatorConfig.from_py_file(x)

    raise ValueError(repr(x))


@attrs.frozen(kw_only=True)
class AggregatorBackendConfig(OpenEoBackendConfig):
    # TODO #112 migrate everything from AggregatorConfig to this class

    capabilities_backend_version: str = openeo_aggregator.about.__version__
    capabilities_deploy_metadata: dict = build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo_aggregator"],
    )

    streaming_chunk_size: int = STREAM_CHUNK_SIZE_DEFAULT

    auth_entitlement_check: Union[bool, dict] = False

    # TTL for connection caching.
    connections_cache_ttl: float = 5 * 60.0

    # List of collection ids to cover with the aggregator (when None: support union of all upstream collections)
    collection_whitelist: Optional[List[Union[str, re.Pattern]]] = None


# Internal singleton
_config_getter = ConfigGetter(expected_class=AggregatorBackendConfig)


def get_backend_config() -> AggregatorBackendConfig:
    """Public config getter"""
    return _config_getter.get()
