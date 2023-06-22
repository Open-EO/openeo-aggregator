"""
openeo-aggregator Flask app
"""
import logging
import os
from typing import Any

import flask
import openeo_driver.views
from openeo_driver.config.load import ConfigGetter
from openeo_driver.util.logging import (
    LOGGING_CONTEXT_FLASK,
    get_logging_config,
    setup_logging,
)
from openeo_driver.utils import smart_bool

from openeo_aggregator.backend import (
    AggregatorBackendImplementation,
    MultiBackendConnection,
)
from openeo_aggregator.config import AggregatorConfig, get_config, get_config_dir

_log = logging.getLogger(__name__)


def create_app(config: Any = None, auto_logging_setup: bool = True) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery

    if auto_logging_setup:
        root_handlers = ["stderr_json"]
        if smart_bool(os.environ.get("OPENEO_AGGREGATOR_SIMPLE_LOGGING")):
            root_handlers = None

        setup_logging(config=get_logging_config(
            root_handlers=root_handlers,
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

    os.environ.setdefault(ConfigGetter.OPENEO_BACKEND_CONFIG, str(get_config_dir() / "backend_config.py"))

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

    app.config.from_mapping(
        # Config hack to make backend_implementation available from pytest fixture
        # TODO: is there another, less hackish way?
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
