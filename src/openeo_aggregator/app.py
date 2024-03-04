"""
openeo-aggregator Flask app
"""

import logging
import os
from pathlib import Path
from typing import List, Optional, Union

import flask
import openeo_driver.views
from openeo_driver.util.logging import (
    LOG_HANDLER_STDERR_JSON,
    LOGGING_CONTEXT_FLASK,
    get_logging_config,
    setup_logging,
)
from openeo_driver.utils import smart_bool

from openeo_aggregator.about import log_version_info
from openeo_aggregator.backend import (
    AggregatorBackendImplementation,
    MultiBackendConnection,
)

_log = logging.getLogger(__name__)


def create_app(auto_logging_setup: bool = True, flask_error_handling: bool = True) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery

    if auto_logging_setup:
        setup_logging(config=get_aggregator_logging_config(context=LOGGING_CONTEXT_FLASK))

    log_version_info(logger=_log)

    backends = MultiBackendConnection.from_config()

    _log.info("Creating AggregatorBackendImplementation")
    backend_implementation = AggregatorBackendImplementation(backends=backends)

    _log.info(f"Building Flask app with {backend_implementation=!r}")
    app = openeo_driver.views.build_app(
        backend_implementation=backend_implementation,
        error_handling=flask_error_handling,
    )

    app.config.from_mapping(
        # Config hack to make backend_implementation available from pytest fixture
        # TODO: is there another, less hackish way?
        OPENEO_BACKEND_IMPLEMENTATION=backend_implementation,
    )

    # Some additional experimental inspection/debug endpoints

    @app.route("/_info", methods=["GET"])
    def agg_backends():
        info = {"backends": [{"id": con.id, "root_url": con.root_url} for con in backends]}
        return flask.jsonify(info)

    _log.info(f"Built {app=!r}")
    return app


def get_aggregator_logging_config(
    *,
    context: str = LOGGING_CONTEXT_FLASK,
    handler_default_level: str = "DEBUG",
    root_handlers: Optional[List[str]] = None,
    log_file: Optional[Union[str, Path]] = None,
) -> dict:
    root_handlers = root_handlers or [LOG_HANDLER_STDERR_JSON]
    if smart_bool(os.environ.get("OPENEO_AGGREGATOR_SIMPLE_LOGGING")):
        root_handlers = None

    return get_logging_config(
        root_handlers=root_handlers,
        handler_default_level=handler_default_level,
        loggers={
            "openeo": {"level": "DEBUG"},
            "openeo_driver": {"level": "DEBUG"},
            "openeo_aggregator": {"level": "DEBUG"},
            "flask": {"level": "INFO"},
            "werkzeug": {"level": "INFO"},
            "gunicorn": {"level": "INFO"},
            "kazoo": {"level": "WARN"},
        },
        context=context,
        log_file=log_file,
    )


if __name__ == "__main__":
    app = create_app()
    app.run()
