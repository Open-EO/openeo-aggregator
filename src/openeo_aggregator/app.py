"""
openeo-aggregator Flask app
"""
import logging
from typing import Any

import flask

import openeo_aggregator.about
import openeo_driver.views
from openeo_driver.util.logging import setup_logging, get_logging_config, LOGGING_CONTEXT_FLASK
from openeo_aggregator.backend import AggregatorBackendImplementation, MultiBackendConnection
from openeo_aggregator.config import get_config, AggregatorConfig
from openeo_driver.server import build_backend_deploy_metadata

_log = logging.getLogger(__name__)


def create_app(config: Any = None, auto_logging_setup: bool = True) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery

    if auto_logging_setup:
        # TODO option to use standard text logging instead of JSON, for local development?
        setup_logging(config=get_logging_config(
            root_handlers=["stderr_json"],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "openeo_aggregator": {"level": "DEBUG"},
                "flask": {"level": "INFO"},
                "werkzeug": {"level": "INFO"},
                "gunicorn": {"level": "INFO"},
                'kazoo': {'level': 'WARN'},
            },
            context=LOGGING_CONTEXT_FLASK,
        ))

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
        OPENEO_BACKEND_IMPLEMENTATION=backend_implementation,
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
    app = create_app()
    app.run()
