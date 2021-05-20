"""
openeo-aggregator Flask app
"""
import flask

import openeo_aggregator.about
import openeo_driver.views
from openeo_aggregator.backend import AggregatorBackendImplementation
from openeo_driver.server import build_backend_deploy_metadata


def create_app() -> flask.Flask:
    """
    Flask application factory function.
    """
    # This `create_app` factory is auto-detected by Flask's application discovery when running `flask run`
    # see https://flask.palletsprojects.com/en/2.0.x/cli/#application-discovery)

    backend_implementation = AggregatorBackendImplementation()
    app = openeo_driver.views.build_app(backend_implementation=backend_implementation)

    deploy_metadata = build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo_aggregator"]
    )

    app.config.from_mapping(
        OPENEO_TITLE="openEO Aggregator Driver",
        OPENEO_DESCRIPTION="openEO Aggregator Driver",
        OPENEO_BACKEND_VERSION=openeo_aggregator.about.__version__,
        OPENEO_BACKEND_DEPLOY_METADATA=deploy_metadata,
    )
    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
