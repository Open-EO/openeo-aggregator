"""
Run openeo-aggregator as gunicorn app
"""

import logging.config

from openeo_aggregator.app import create_app
from openeo_driver.server import run_gunicorn
from openeo_driver.server import show_log_level

_log = logging.getLogger(__name__)


def main():
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

    app = create_app()

    # TODO: eliminate this boilerplate?
    show_log_level(logging.getLogger("openeo"))
    show_log_level(logging.getLogger("openeo_driver"))
    show_log_level(app.logger)

    run_gunicorn(
        app,
        # TODO: these are localhost settings for now. Add (cli) options to set this?
        threads=2,
        host="127.0.0.1",
        port=8080,
    )


if __name__ == "__main__":
    main()
