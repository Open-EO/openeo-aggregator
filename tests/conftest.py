import flask
import pytest

from openeo_aggregator.app import create_app
from openeo_aggregator.backend import MultiBackendConnection
from openeo_aggregator.config import AggregatorConfig
from openeo_driver.testing import ApiTester
from openeo_driver.users.oidc import OidcProvider


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
def base_config() -> AggregatorConfig:
    """Base config for tests (without any configured backends)."""
    conf = AggregatorConfig()
    # conf.flask_error_handling = False  # Temporary disable flask error handlers to simplify debugging (better stack traces).

    conf.configured_oidc_providers = [
        OidcProvider(id="egi", issuer="https://egi.test", title="EGI"),
        OidcProvider(id="x-agg", issuer="https://x.test", title="X (agg)"),
        OidcProvider(id="y-agg", issuer="https://y.test", title="Y (agg)"),
        OidcProvider(id="z-agg", issuer="https://z.test", title="Z (agg)"),
    ]

    # Disable OIDC/EGI entitlement check by default.
    conf.auth_entitlement_check = False
    return conf


@pytest.fixture
def config(base_config, backend1, backend2) -> AggregatorConfig:
    """Config for most tests with two backends."""
    conf = base_config
    conf.aggregator_backends = {
        "b1": backend1,
        "b2": backend2,
    }
    return conf


def get_flask_app(config: AggregatorConfig) -> flask.Flask:
    app = create_app(config=config, auto_logging_setup=False)
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeoa.test'
    return app


@pytest.fixture
def flask_app(config: AggregatorConfig) -> flask.Flask:
    return get_flask_app(config)


def get_api100(flask_app: flask.Flask) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=flask_app.test_client())


@pytest.fixture
def api100(flask_app: flask.Flask) -> ApiTester:
    return get_api100(flask_app)


@pytest.fixture
def api100_with_entitlement_check(config: AggregatorConfig) -> ApiTester:
    config.auth_entitlement_check = {"oidc_issuer_whitelist": {"https://egi.test", "https://egi.test/oidc"}}
    return get_api100(get_flask_app(config))


def assert_dict_subset(d1: dict, d2: dict):
    """Check whether dictionary `d1` is a subset of `d2`"""
    assert d1 == {k: v for (k, v) in d2.items() if k in d1}
