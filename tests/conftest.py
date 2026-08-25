import os
from pathlib import Path
from typing import Callable

import flask
import pytest
from openeo_driver.testing import ApiTester
from openeo_driver.views import OPENEO_API_VERSION_DEFAULT

from openeo_aggregator.app import create_app
from openeo_aggregator.backend import (
    AggregatorBackendImplementation,
    AggregatorCollectionCatalog,
    MultiBackendConnection,
)
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
    requests_mock.get(
        domain + "/",
        json=mbldr.capabilities(
            title="Dummy Federation One",
            description="Welcome to Federation One.",
        ),
    )
    requests_mock.get(domain + "/credentials/oidc", json=mbldr.credentials_oidc())
    requests_mock.get(domain + "/processes", json=mbldr.processes(*_DEFAULT_PROCESSES))
    return domain


@pytest.fixture
def backend2(requests_mock, mbldr) -> str:
    domain = "https://b2.test/v1"
    requests_mock.get(
        domain + "/",
        json=mbldr.capabilities(
            title="Dummy The Second",
        ),
    )
    requests_mock.get(domain + "/credentials/oidc", json=mbldr.credentials_oidc())
    requests_mock.get(domain + "/processes", json=mbldr.processes(*_DEFAULT_PROCESSES))
    return domain


@pytest.fixture
def zk_client() -> DummyKazooClient:
    return DummyKazooClient()


@pytest.fixture
def backend1_id() -> str:
    """Id of first upstream backend. As a fixture to allow per-test override"""
    return "b1"


@pytest.fixture
def backend2_id() -> str:
    """Id of second upstream backend. As a fixture to allow per-test override"""
    return "b2"


@pytest.fixture
def multi_backend_connection(backend1, backend2) -> MultiBackendConnection:
    return MultiBackendConnection.from_config()


def get_flask_app() -> flask.Flask:
    app = create_app(
        auto_logging_setup=False,
        # flask_error_handling=False,  # Failing test debug tip: set to False for deeper stack trace insights
    )
    app.config["TESTING"] = True
    app.config["SERVER_NAME"] = "oeoa.test"
    return app


@pytest.fixture
def flask_app(backend1, backend2) -> flask.Flask:
    app = get_flask_app()
    with app.app_context():
        yield app


@pytest.fixture
def backend_implementation(flask_app) -> AggregatorBackendImplementation:
    """Get AggregatorBackendImplementation from flask app"""
    return flask_app.config["OPENEO_BACKEND_IMPLEMENTATION"]


@pytest.fixture(
    params=[
        # Note: this just lists the default openEO API version,
        # but allows ad-hoc running against another/future version(s)
        OPENEO_API_VERSION_DEFAULT,
    ]
)
def api_version(request) -> str:
    return request.param


@pytest.fixture
def api(flask_app: flask.Flask, api_version) -> ApiTester:
    """openEO API fixture with default openEO version"""
    return ApiTester(api_version=api_version, client=flask_app.test_client())


@pytest.fixture
def get_api(api_version) -> Callable[[], ApiTester]:
    """
    Just-in-time construction of API flask app tester (to allow config customization at test call phase)
    """

    def get() -> ApiTester:
        flask_app = get_flask_app()
        return ApiTester(api_version=api_version, client=flask_app.test_client())

    return get


@pytest.fixture
def api_with_entitlement_check(get_api) -> ApiTester:
    # TODO: still necessary to cover this (now) unused "entitlement" feature?
    with config_overrides(
        auth_entitlement_check={"oidc_issuer_whitelist": {"https://egi.test", "https://egi.test/oidc"}}
    ):
        yield get_api()


@pytest.fixture
def catalog(multi_backend_connection) -> AggregatorCollectionCatalog:
    return AggregatorCollectionCatalog(backends=multi_backend_connection)


@pytest.fixture
def mbldr() -> MetadataBuilder:
    """Metadata builder"""
    return MetadataBuilder()
