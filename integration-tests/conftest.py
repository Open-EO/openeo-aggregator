import os

import openeo
import pytest
import requests
from openeo.capabilities import ComparableVersion


def get_openeo_base_url(version: str = "1.1.0"):
    try:
        endpoint = os.environ["ENDPOINT"].rstrip("/")
    except Exception:
        raise RuntimeError(
            "Environment variable 'ENDPOINT' should be set"
            " with URL pointing to OpenEO backend to test against"
            " (e.g. 'http://localhost:8080/' or 'http://openeo-dev.vgt.vito.be/')"
        )
    return "{e}/openeo/{v}".format(e=endpoint.rstrip("/"), v=version)


@pytest.fixture(
    params=[
        "1.1.0",
    ]
)
def api_version(request) -> ComparableVersion:
    return ComparableVersion(request.param)


@pytest.fixture
def api_base_url(api_version):
    return get_openeo_base_url(str(api_version))


@pytest.fixture
def requests_session(request) -> requests.Session:
    """
    Fixture to create a `requests.Session` that automatically injects a query parameter in API URLs
    referencing the currently running test.
    Simplifies cross-referencing between integration tests and flask/YARN logs
    """
    session = requests.Session()
    session.params["_origin"] = f"{request.session.name}/{request.node.name}"
    return session


@pytest.fixture
def connection(api_base_url, requests_session) -> openeo.Connection:
    return openeo.connect(api_base_url, session=requests_session)


@pytest.fixture
def connection100(requests_session) -> openeo.Connection:
    return openeo.connect(get_openeo_base_url("1.0.0"), session=requests_session)


# TODO: real authentication?
TEST_USER = "jenkins"
TEST_PASSWORD = TEST_USER + "123"


@pytest.fixture
def auth_connection(connection) -> openeo.Connection:
    """Connection fixture to a backend of given version with some image collections."""
    connection.authenticate_basic(TEST_USER, TEST_PASSWORD)
    return connection
