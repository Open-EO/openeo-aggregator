import logging
import os
import re
from pathlib import Path
from typing import Any, List, Union

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.utils import dict_item

_log = logging.getLogger(__name__)

OPENEO_AGGREGATOR_CONFIG = "OPENEO_AGGREGATOR_CONFIG"
ENVIRONMENT_INDICATOR = "ENV"

CACHE_TTL_DEFAULT = 6 * 60 * 60

# Timeouts for requests to back-ends
CONNECTION_TIMEOUT_DEFAULT = 30
CONNECTION_TIMEOUT_INIT = 12.5
CONNECTION_TIMEOUT_RESULT = 15 * 60
CONNECTION_TIMEOUT_JOB_START = 5 * 60

STREAM_CHUNK_SIZE_DEFAULT = 10 * 1024


class ConfigException(ValueError):
    pass


class AggregatorConfig(dict):
    """
    Simple dictionary based configuration for aggregator backend
    """
    config_source = dict_item()

    # Dictionary mapping backend id to backend url
    aggregator_backends = dict_item()

    flask_error_handling = dict_item(default=True)
    streaming_chunk_size = dict_item(default=STREAM_CHUNK_SIZE_DEFAULT)

    # TODO: add validation/normalization to make sure we have a real list of OidcProvider objects?
    configured_oidc_providers: List[OidcProvider] = dict_item(default=[])
    auth_entitlement_check: Union[bool, dict] = dict_item()

    partitioned_job_tracking = dict_item(default=None)
    zookeeper_prefix = dict_item(default="/openeo-aggregator/")

    # See `memoizer_from_config` for details.
    memoizer = dict_item(default={"type": "dict"})

    # TTL for connection caching.
    connections_cache_ttl = dict_item(default=5 * 60.0)

    @staticmethod
    def from_py_file(path: Union[str, Path]) -> 'AggregatorConfig':
        """Load config from Python file."""
        path = Path(path)
        _log.info(f"Loading config from Python file {path}")
        # Based on flask's Config.from_pyfile
        with path.open(mode="rb") as f:
            code = compile(f.read(), path, "exec")
        globals = {"__file__": str(path)}
        exec(code, globals)
        try:
            config = globals["config"]
        except KeyError:
            raise ConfigException(f"No 'config' variable defined in config file {path}")
        if not isinstance(config, AggregatorConfig):
            raise ConfigException(f"Variable 'config' from {path} is not AggregatorConfig but {type(config)}")
        return config

    def copy(self) -> 'AggregatorConfig':
        return AggregatorConfig(self)


def get_config_dir() -> Path:
    # TODO: make this robust against packaging operations (e.g. no guaranteed real __file__ path)
    for root in [Path.cwd(), Path(__file__).parent.parent.parent]:
        config_dir = root / "conf"
        if config_dir.is_dir():
            return config_dir
    raise RuntimeError("No config dir found")


def get_config(x: Any = None) -> AggregatorConfig:
    """
    Get aggregator config from given object:
    - if None: check env variable "OPENEO_AGGREGATOR_CONFIG" or return default config
    - if it is already an `AggregatorConfig` object: return as is
    - if it is a string: try to parse it as JSON (file)
    """

    if x is None:
        for env_var in [OPENEO_AGGREGATOR_CONFIG, ENVIRONMENT_INDICATOR]:
            if env_var in os.environ:
                x = os.environ[env_var]
                _log.info(f"Config from env var {env_var}: {x!r}")
                break
        else:
            x = "dev"

    if isinstance(x, str):
        if re.match("^[a-zA-Z]+$", x):
            x = get_config_dir() / f"aggregator.{x.lower()}.py"
        x = Path(x)

    if isinstance(x, AggregatorConfig):
        return x
    elif isinstance(x, Path) and x.suffix.lower() == ".py":
        return AggregatorConfig.from_py_file(x)

    raise ValueError(repr(x))
