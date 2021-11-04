"""
openeo-aggregator Flask app
"""
import logging
from typing import Any

import flask

import openeo_aggregator.about
import openeo_driver.views
from openeo_aggregator.backend import AggregatorBackendImplementation, MultiBackendConnection
from openeo_aggregator.config import get_config, AggregatorConfig
from openeo_driver.server import build_backend_deploy_metadata, setup_logging

_log = logging.getLogger(__name__)


def create_app(config: Any = None, auto_logging_setup=True) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery

    if auto_logging_setup:
        setup_logging(
            loggers={"openeo_aggregator": {"level": "INFO"}},
            show_loggers=["openeo_driver", "openeo_aggregator"]
        )

    config: AggregatorConfig = get_config(config)
    _log.info(f"Using config: {config}")

    _log.info(f"Creating MultiBackendConnection with {config.aggregator_backends}")
    backends = MultiBackendConnection.from_config(config)

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
        OPENEO_TITLE="openEO Platform",
        OPENEO_DESCRIPTION="openEO Platform, provided through openEO Aggregator Driver",
        OPENEO_BACKEND_VERSION=openeo_aggregator.about.__version__,
        OPENEO_BACKEND_DEPLOY_METADATA=deploy_metadata,
    )

    # Some additional experimental inspection/debug endpoints

    @app.route("/_info", methods=["GET"])
    def agg_backends():
        info = {
            "backends": [{"id": con.id, "root_url": con.root_url} for con in backends]
        }
        return flask.jsonify(info)

    _log.info(f"Built app {app}")
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    app.run()
