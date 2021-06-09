import flask

from openeo_aggregator.app import create_app
from openeo_aggregator.config import AggregatorConfig


def test_create_app(config: AggregatorConfig):
    app = create_app(config)
    assert isinstance(app, flask.Flask)
