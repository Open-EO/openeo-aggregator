import logging
import logging.config
import os
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
CONNECTION_TIMEOUT_RESULT = 15 * 60

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
    return Path.cwd() / "conf"


def get_config(x: Any) -> AggregatorConfig:
    """
    Get aggregator config from given object:
    - if None: check env variable "OPENEO_AGGREGATOR_CONFIG" or return default config
    - if it is already an `AggregatorConfig` object: return as is
    - if it is a string: try to parse it as JSON (file)
    """
    if x is None:
        if OPENEO_AGGREGATOR_CONFIG in os.environ:
            x = os.environ[OPENEO_AGGREGATOR_CONFIG]
            _log.info(f"Config file from env var {OPENEO_AGGREGATOR_CONFIG}: {x!r}")
        else:
            # TODO: just use OPENEO_AGGREGATOR_CONFIG feature for this?
            env = os.environ.get(ENVIRONMENT_INDICATOR, "dev").lower()
            x = get_config_dir() / f"aggregator.{env}.py"
            _log.info(f"Config file for env {env!r}: {x}")

    if isinstance(x, AggregatorConfig):
        return x
    elif isinstance(x, (str, Path)) and Path(x).suffix.lower() == '.py':
        return AggregatorConfig.from_py_file(x)

    raise ValueError(repr(x))


def setup_logging(force=False):
    # TODO option to use standard text logging instead of JSON, for local development?
    if not logging.getLogger().handlers or force:
        config_file = get_config_dir() / "logging-json.conf"
        logging.config.fileConfig(config_file, disable_existing_loggers=False)

    for name in [
        "openeo", "openeo_aggregator", "openeo_driver",
        "flask", "werkzeug",
    ]:
        logger = logging.getLogger(name)
        logger.log(level=logger.getEffectiveLevel(), msg=f"Logger setup: {logger!r}")
