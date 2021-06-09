"""
openeo-aggregator Flask app
"""
import logging
import os
from pathlib import Path
from typing import Any
import urllib.parse
import flask

import openeo_aggregator.about
import openeo_driver.views
from openeo_aggregator.backend import AggregatorBackendImplementation, MultiBackendConnection
from openeo_aggregator.config import AggregatorConfig, DEFAULT_CONFIG
from openeo_driver.server import build_backend_deploy_metadata, setup_logging

_log = logging.getLogger(__name__)

OPENEO_AGGREGATOR_CONFIG = "OPENEO_AGGREGATOR_CONFIG"


def get_config(x: Any) -> AggregatorConfig:
    """
    Get aggregator config from given object:
    - if None: check env variable "OPENEO_AGGREGATOR_CONFIG" or return default config
    - if it is already an `AggregatorConfig` object: return as is
    - if it is a string: try to parse it as JSON (file)
    """
    if x is None:
        x = os.environ.get(OPENEO_AGGREGATOR_CONFIG, DEFAULT_CONFIG)

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


def create_app(config: Any = None) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery

    config = get_config(config)

    if config.auto_logging_setup:
        setup_logging(
            loggers={"openeo_aggregator": {"level": "INFO"}},
            show_loggers=["openeo_driver", "openeo_aggregator"]
        )

    _log.info(f"Creating MultiBackendConnection with {config.aggregator_backends}")
    backends = MultiBackendConnection(backends=config.aggregator_backends)

    _log.info("Creating AggregatorBackendImplementation")
    backend_implementation = AggregatorBackendImplementation(backends=backends, config=config)

    _log.info("Building Flask app")
    app = openeo_driver.views.build_app(
        backend_implementation=backend_implementation,
        error_handling=config.flask_error_handling,
    )

    deploy_metadata = build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo_aggregator"],
    )
    app.config.from_mapping(
        OPENEO_TITLE="openEO Aggregator Driver",
        OPENEO_DESCRIPTION="openEO Aggregator Driver",
        OPENEO_BACKEND_VERSION=openeo_aggregator.about.__version__,
        OPENEO_BACKEND_DEPLOY_METADATA=deploy_metadata,
    )

    _log.info(f"Built app {app}")
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    app.run()
