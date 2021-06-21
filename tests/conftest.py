import flask
import pytest

from openeo_aggregator.app import create_app
from openeo_aggregator.backend import MultiBackendConnection
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
def multi_backend_connection(backend1, backend2) -> MultiBackendConnection:
    return MultiBackendConnection({
        "b1": backend1,
        "b2": backend2,
    })


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
    app = create_app(config=config, auto_logging_setup=False)
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeoa.test'
    return app


def assert_dict_subset(d1: dict, d2: dict):
    """Check whether dictionary `d1` is a subset of `d2`"""
    assert d1 == {k: v for (k, v) in d2.items() if k in d1}
