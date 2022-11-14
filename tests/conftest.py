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


# as "lib_requests_mock": make a distinction with the pytest fixture that has the same name 
def set_backend_to_api_version(requests_mock, domain: str, api_version: str) -> str:
    """Helper function to make the backend connection use the expected API version."""

    # TODO: would like a nicer solution to make the backend fixtures match the expected API version.
    # Good enough for now tough, just have to remember to call it in your test.
    return requests_mock.get(f"{domain}/", json={"api_version": api_version})


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
def connections_cache_ttl() -> float:
    """MultiBackendConnection.connections_cache_ttl fixture to allow parameterization"""
    return 1.0


@pytest.fixture
def base_config(
    configured_oidc_providers, zk_client, memoizer_config, connections_cache_ttl
) -> AggregatorConfig:
    """Base config for tests (without any configured backends)."""
    conf = AggregatorConfig()
    # conf.flask_error_handling = False  # Temporary disable flask error handlers to simplify debugging (better stack traces).

    conf.configured_oidc_providers = configured_oidc_providers
    # Disable OIDC/EGI entitlement check by default.
    conf.auth_entitlement_check = False

    conf.memoizer = memoizer_config
    conf.connections_cache_ttl = connections_cache_ttl

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


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version_fixture(request):
    """To go through all relevant API versions"""
    return request.param


def get_api_tester(flask_app, api_version) -> ApiTester:
    return ApiTester(api_version=api_version, client=flask_app.test_client())


def get_api040(flask_app: flask.Flask) -> ApiTester:
    return ApiTester(api_version="0.4.0", client=flask_app.test_client())


def get_api100(flask_app: flask.Flask) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=flask_app.test_client())


@pytest.fixture
def api_tester(flask_app, api_version_fixture) -> ApiTester:
    """Get an ApiTester for each version.

    Useful when it easy to test several API versions with (mostly) the same test code.
    But when the difference is too big, just keep it simple and write separate tests.
    """
    return get_api_tester(flask_app, api_version_fixture)


@pytest.fixture
def api040(flask_app: flask.Flask) -> ApiTester:
    return get_api040(flask_app)


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
