"""
openeo-aggregator Flask app
"""
import logging
import logging.config
import time
from typing import List

import flask

import openeo_aggregator.about
import openeo_driver.views
from openeo_aggregator.backend import AggregatorBackendImplementation, MultiBackendConnection
from openeo_aggregator.config import AggregatorConfig, DEFAULT_CONFIG
from openeo_driver.server import build_backend_deploy_metadata, show_log_level


def create_app(config: AggregatorConfig = DEFAULT_CONFIG) -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery)

    if config.auto_logging_setup:
        setup_logging(show_loggers=["openeo_driver", "openeo_aggregator"])

    backends = MultiBackendConnection(backends=config.aggregator_backends)
    backend_implementation = AggregatorBackendImplementation(backends=backends, config=config)
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
    return app


class UtcLogFormatter(logging.Formatter):
    """Log formatter that uses UTC instead of local time."""
    # based on https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration
    converter = time.gmtime


def setup_logging(root_level="INFO", show_loggers: List[str] = ["openeo_driver"]):
    # TODO: move this to openeo_driver
    # Based on https://flask.palletsprojects.com/en/2.0.x/logging/
    config = {
        'version': 1,
        'formatters': {
            'utclogformatter': {
                '()': UtcLogFormatter,
                'format': '[%(asctime)s] %(process)s %(levelname)s in %(name)s: %(message)s',
            }
        },
        'handlers': {
            'wsgi': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://flask.logging.wsgi_errors_stream',
                'formatter': 'utclogformatter'
            }
        },
        'root': {
            'level': root_level,
            'handlers': ['wsgi']
        },
        'loggers': {
            'werkzeug': {'level': 'INFO'},
            'flask': {'level': 'INFO'},
            'openeo': {'level': 'INFO'},
            'openeo_driver': {'level': 'DEBUG'},
            'openeo_aggregator': {'level': 'DEBUG'},
            'kazoo': {'level': 'WARN'},
        }
    }
    logging.config.dictConfig(config)

    for name in show_loggers:
        show_log_level(logging.getLogger(name))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    app.run()
