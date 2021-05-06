"""
Run openeo-aggregator as gunicorn app
"""

import logging.config

import openeo_aggregator.about
from openeo_driver import server
from openeo_driver.server import show_log_level

_log = logging.getLogger(__name__)

if __name__ == '__main__':
    # TODO: move this logging config boilerplate to a openeo_driver helper function
    logging.config.dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] %(process)s %(levelname)s in %(name)s: %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        },
        'loggers': {
            'werkzeug': {'level': 'DEBUG'},
            'flask': {'level': 'DEBUG'},
            'openeo': {'level': 'DEBUG'},
            'openeo_driver': {'level': 'DEBUG'},
            'openeo_aggregator': {'level': 'DEBUG'},
            'kazoo': {'level': 'WARN'},
        }
    })

    from openeo_aggregator.app import app

    # TODO: eliminate this boilerplate?
    show_log_level(logging.getLogger('openeo'))
    show_log_level(logging.getLogger('openeo_driver'))
    show_log_level(app.logger)

    deploy_metadata = server.build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo_aggregator"]
    )
    server.run(
        title="openEO Aggregator Driver",
        description="openEO Aggregator Driver",
        deploy_metadata=deploy_metadata,
        backend_version=openeo_aggregator.about.__version__,
        # TODO: these are localhost settings for now. Add (cli) options to set this?
        threads=2,
        host="127.0.0.1",
        port=8080
    )
