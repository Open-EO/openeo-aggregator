import flask
import pytest
from flask.testing import FlaskClient

from openeo_aggregator.app import create_app
from openeo_aggregator.config import AggregatorConfig


@pytest.fixture
def backend1(requests_mock):
    domain = "https://b1.test/v1"
    # TODO: how to work with different API versions?
    requests_mock.get(domain + "/", json={"api_version": "1.0.0"})
    requests_mock.get(domain + "/credentials/oidc", json={"providers": [
        {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
    ]})
    return domain


@pytest.fixture
def backend2(requests_mock):
    domain = "https://b2.test/v1"
    requests_mock.get(domain + "/", json={"api_version": "1.0.0"})
    requests_mock.get(domain + "/credentials/oidc", json={"providers": [
        {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
    ]})
    return domain


@pytest.fixture
def config(backend1, backend2) -> AggregatorConfig:
    conf = AggregatorConfig()
    conf.aggregator_backends = {
        "b1": backend1,
        "b2": backend2,
    }
    # conf.flask_error_handling = False  # Temporary disable flask error handlers to simplify debugging (better stack traces).

    return conf


@pytest.fixture
def flask_app(config: AggregatorConfig) -> flask.Flask:
    app = create_app(config=config)
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeoa.test'
    return app


@pytest.fixture
def client(flask_app) -> FlaskClient:
    return flask_app.test_client()
