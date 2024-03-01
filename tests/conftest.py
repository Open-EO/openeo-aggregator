import os
from pathlib import Path
from typing import List

import flask
import pytest
from openeo_driver.testing import ApiTester
from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.app import create_app
from openeo_aggregator.backend import (
    AggregatorBackendImplementation,
    AggregatorCollectionCatalog,
    MultiBackendConnection,
)
from openeo_aggregator.config import AggregatorConfig
from openeo_aggregator.testing import (
    DummyKazooClient,
    MetadataBuilder,
    config_overrides,
)

pytest_plugins = "pytester"


def pytest_configure(config):
    """Pytest configuration hook"""

    # Load test specific config
    os.environ["OPENEO_BACKEND_CONFIG"] = str(Path(__file__).parent / "backend_config.py")




_DEFAULT_PROCESSES = [
    "load_collection",
    "load_result",
    "save_result",
    "merge_cubes",
    "mask",
    "load_ml_model",
    "add",
    "large",
]


@pytest.fixture
def backend1(requests_mock, mbldr) -> str:
    domain = "https://b1.test/v1"
    # TODO: how to work with different API versions?
    requests_mock.get(domain + "/", json=mbldr.capabilities())
    requests_mock.get(domain + "/credentials/oidc", json=mbldr.credentials_oidc())
    requests_mock.get(domain + "/processes", json=mbldr.processes(*_DEFAULT_PROCESSES))
    return domain


@pytest.fixture
def backend2(requests_mock, mbldr) -> str:
    domain = "https://b2.test/v1"
    requests_mock.get(domain + "/", json=mbldr.capabilities())
    requests_mock.get(domain + "/credentials/oidc", json=mbldr.credentials_oidc())
    requests_mock.get(domain + "/processes", json=mbldr.processes(*_DEFAULT_PROCESSES))
    return domain


@pytest.fixture
def zk_client() -> DummyKazooClient:
    return DummyKazooClient()




@pytest.fixture
def base_config(zk_client) -> AggregatorConfig:
    """Base config for tests (without any configured backends)."""
    conf = AggregatorConfig()
    conf.config_source = "test fixture base_config"
    # conf.flask_error_handling = False  # Temporary disable flask error handlers to simplify debugging (better stack traces).


    return conf


@pytest.fixture
def backend1_id() -> str:
    """Id of first upstream backend. As a fixture to allow per-test override"""
    return "b1"


@pytest.fixture
def backend2_id() -> str:
    """Id of second upstream backend. As a fixture to allow per-test override"""
    return "b2"


@pytest.fixture
def config(
    base_config, backend1, backend2, backend1_id, backend2_id
) -> AggregatorConfig:
    """Config for most tests with two backends."""
    conf = base_config
    return conf


@pytest.fixture
def multi_backend_connection(config) -> MultiBackendConnection:
    return MultiBackendConnection.from_config(config)


def get_flask_app(config: AggregatorConfig) -> flask.Flask:
    app = create_app(
        config=config,
        auto_logging_setup=False,
        # flask_error_handling=False,  # Failing test debug tip: set to False for deeper stack trace insights
    )
    app.config["TESTING"] = True
    app.config["SERVER_NAME"] = "oeoa.test"
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
    with config_overrides(
        auth_entitlement_check={"oidc_issuer_whitelist": {"https://egi.test", "https://egi.test/oidc"}}
    ):
        yield get_api100(get_flask_app(config))


def assert_dict_subset(d1: dict, d2: dict):
    """Check whether dictionary `d1` is a subset of `d2`"""
    assert d1 == {k: v for (k, v) in d2.items() if k in d1}


@pytest.fixture
def catalog(multi_backend_connection, config) -> AggregatorCollectionCatalog:
    return AggregatorCollectionCatalog(
        backends=multi_backend_connection,
        config=config
    )


@pytest.fixture
def mbldr() -> MetadataBuilder:
    """Metadata builder"""
    return MetadataBuilder()
