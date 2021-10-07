import json
import logging
import os
import urllib.parse
from pathlib import Path
from typing import Any, List
from typing import Union

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.utils import dict_item

_log = logging.getLogger(__name__)

OPENEO_AGGREGATOR_CONFIG = "OPENEO_AGGREGATOR_CONFIG"

CACHE_TTL_DEFAULT = 6 * 60 * 60

CONNECTION_TIMEOUT_DEFAULT = 30
CONNECTION_TIMEOUT_RESULT = 15 * 60

STREAM_CHUNK_SIZE_DEFAULT = 10 * 1024


class AggregatorConfig(dict):
    """
    Simple dictionary based configuration for aggregator backend
    """

    # Dictionary mapping backend id to backend url
    aggregator_backends = dict_item()

    flask_error_handling = dict_item(default=True)
    streaming_chunk_size = dict_item(default=STREAM_CHUNK_SIZE_DEFAULT)

    # TODO: add validation/normalization to make sure we have a real list of OidcProvider objects?
    configured_oidc_providers: List[OidcProvider] = dict_item(default=[])
    auth_entitlement_check = dict_item(default=True)

    @classmethod
    def from_json(cls, data: str):
        return cls(json.loads(data))

    @classmethod
    def from_json_file(cls, path: Union[str, Path]):
        with Path(path).open() as f:
            return cls(json.load(f))


_DEFAULT_OIDC_CLIENT_EGI = {
    "id": "openeo-platform-default-client",
    "grant_types": [
        "authorization_code+pkce",
        "urn:ietf:params:oauth:grant-type:device_code+pkce",
        "refresh_token",
    ]
}

DEFAULT_CONFIG = AggregatorConfig(
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.0/",
        # "creo": "https://openeo.creo.vito.be/openeo/1.0/",
        "eodc": "https://openeo.eodc.eu/v1.0/",
        # "eodcdev": "https://openeo-dev.eodc.eu/v1.0/",
    },
    auth_entitlement_check=True,
    configured_oidc_providers=[
        OidcProvider(
            id="egi",
            issuer="https://aai.egi.eu/oidc/",
            scopes=[
                "openid", "email",
                "eduperson_entitlement",
                "eduperson_scoped_affiliation",
            ],
            title="EGI Check-in",
            default_client=_DEFAULT_OIDC_CLIENT_EGI,  # TODO: remove this legacy experimental field
            default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
        ),
        OidcProvider(
            id="egi-dev",
            issuer="https://aai-dev.egi.eu/oidc/",
            scopes=[
                "openid", "email",
                "eduperson_entitlement",
                "eduperson_scoped_affiliation",
            ],
            title="EGI Check-in (dev)",
            default_client=_DEFAULT_OIDC_CLIENT_EGI,  # TODO: remove this legacy experimental field
            default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
        ),
    ],
)


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
            _log.info(f"Loading config from env var {OPENEO_AGGREGATOR_CONFIG}: {x!r}")
        else:
            x = DEFAULT_CONFIG
            _log.info(f"Using default config: {x}")

    if isinstance(x, AggregatorConfig):
        return x
    elif isinstance(x, str) and x.strip().startswith("{") and x.strip().endswith("}"):
        # Assume it's a JSON dump
        return AggregatorConfig.from_json(x)
    elif isinstance(x, str) and x.strip().lower().startswith("%7b") and x.strip().lower().endswith("%7d"):
        # Assume it's a URL-encoded JSON dump
        x = urllib.parse.unquote(x)
        return AggregatorConfig.from_json(x)
    elif isinstance(x, (str, Path)) and Path(x).suffix.lower() == ".json":
        # Assume it's a path to a JSON file
        return AggregatorConfig.from_json_file(x)

    raise ValueError(repr(x))
