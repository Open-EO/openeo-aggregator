from typing import List

import flask
import pytest

from openeo_aggregator.app import create_app
from openeo_aggregator.backend import MultiBackendConnection, AggregatorBackendImplementation, \
    AggregatorCollectionCatalog
from openeo_aggregator.config import AggregatorConfig
from openeo_aggregator.testing import DummyKazooClient
from openeo_driver.testing import ApiTester
from openeo_driver.users.oidc import OidcProvider


@pytest.fixture
def backend1(requests_mock) -> str:
    domain = "https://b1.test/v1"
    # TODO: how to work with different API versions?
    requests_mock.get(domain + "/", json={"api_version": "1.0.0"})
    requests_mock.get(domain + "/credentials/oidc", json={"providers": [
        {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
    ]})
    return domain


@pytest.fixture
def backend2(requests_mock) -> str:
    domain = "https://b2.test/v1"
    requests_mock.get(domain + "/", json={"api_version": "1.0.0"})
    requests_mock.get(domain + "/credentials/oidc", json={"providers": [
        {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
    ]})
    return domain


@pytest.fixture
def main_test_oidc_issuer() -> str:
    """
    Main OIDC issuer URL.
    As a fixture to make it overridable with `pytest.mark.parametrize` for certain tests.
    """
    return "https://egi.test"


@pytest.fixture
def configured_oidc_providers(main_test_oidc_issuer: str) -> List[OidcProvider]:
    return [
        OidcProvider(id="egi", issuer=main_test_oidc_issuer, title="EGI"),
        OidcProvider(id="x-agg", issuer="https://x.test", title="X (agg)"),
        OidcProvider(id="y-agg", issuer="https://y.test", title="Y (agg)"),
        OidcProvider(id="z-agg", issuer="https://z.test", title="Z (agg)"),
    ]


@pytest.fixture
def zk_client() -> DummyKazooClient:
    return DummyKazooClient()


DEFAULT_MEMOIZER_CONFIG = {
    "type": "dict",
    "config": {"default_ttl": 66},
}


@pytest.fixture
def memoizer_config() -> dict:
    """
    Fixture for global memoizer config, to allow overriding/parameterizing it for certain tests.
    Also see https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#override-a-fixture-with-direct-test-parametrization
    """
    return DEFAULT_MEMOIZER_CONFIG


@pytest.fixture
def base_config(configured_oidc_providers, zk_client, memoizer_config) -> AggregatorConfig:
    """Base config for tests (without any configured backends)."""
    conf = AggregatorConfig()
    # conf.flask_error_handling = False  # Temporary disable flask error handlers to simplify debugging (better stack traces).

    conf.configured_oidc_providers = configured_oidc_providers
    # Disable OIDC/EGI entitlement check by default.
    conf.auth_entitlement_check = False

    conf.memoizer = memoizer_config

    conf.zookeeper_prefix = "/o-a/"
    conf.partitioned_job_tracking = {
        "zk_client": zk_client,
    }
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


@pytest.fixture
def multi_backend_connection(config) -> MultiBackendConnection:
    return MultiBackendConnection.from_config(config)


def get_flask_app(config: AggregatorConfig) -> flask.Flask:
    app = create_app(config=config, auto_logging_setup=False)
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeoa.test'
    return app


@pytest.fixture
def flask_app(config: AggregatorConfig) -> flask.Flask:
    app = get_flask_app(config)
    with app.app_context():
        yield app


@pytest.fixture
def backend_implementation(flask_app) -> AggregatorBackendImplementation:
    """Get AggregatorBackendImplementation from flask app"""
    return flask_app.config["OPENEO_BACKEND_IMPLEMENTATION"]


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


@pytest.fixture
def catalog(multi_backend_connection, config) -> AggregatorCollectionCatalog:
    return AggregatorCollectionCatalog(
        backends=multi_backend_connection,
        config=config
    )
