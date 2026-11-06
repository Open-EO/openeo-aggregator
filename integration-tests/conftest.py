import os

import openeo
import pytest
import requests


@pytest.fixture
def api_base_url():
    try:
        url = os.environ["OPENEO_BACKEND_URL"].rstrip("/")
    except Exception:
        raise RuntimeError(
            "Environment variable 'OPENEO_BACKEND_URL' should point to OpenEO backend URL to test against"
        )
    return url


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
