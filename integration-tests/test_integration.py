import logging

import openeo
import pytest

_log = logging.getLogger(__name__)


def test_openeo_cloud_root_return_sensible_response(connection: openeo.Connection):
    """Check that ${OPENEO_BACKEND_URL}/ returns something sensible."""

    path = "/"
    response = connection.get(path)

    _log.info("As curl:\n" + connection.as_curl(data={}, path=path, method="GET"))
    _log.info(f"{response=}")
    _log.info(f"{response.json()=}")

    response_body = response.json()
    assert response.status_code == 200

    required_keys = [
        "api_version",
        "backend_version",
        "billing",
        "description",
        "endpoints",
        "federation",
        "id",
        "links",
        "processing:software",
        "production",
        "stac_extensions",
        "stac_version",
        "title",
        "version",
    ]
    actual_keys_in_response = response_body.keys()
    assert all([k in actual_keys_in_response for k in required_keys])


def test_collections(connection):
    """Check that GET /collections looks OK"""

    path = "/collections"
    response = connection.get(path)

    _log.info("As curl:\n" + connection.as_curl(data={}, path=path, method="GET"))
    _log.info(f"{response=}")

    # This is quite a lot of information, a bit too long for INFO.
    _log.debug(f"{response.json()=}")

    data = response.json()
    assert "collections" in data

    # Verify that there are indeed processes in the list
    assert data["collections"]

    assert response.status_code == 200


def test_processes(connection):
    """Check that GET /processes looks OK"""

    path = "/processes"
    response = connection.get(path)

    _log.info("As curl:\n" + connection.as_curl(data={}, path=path, method="GET"))
    _log.info(f"{response=}")

    # This is quite a lot of information, a bit too long for INFO.
    _log.debug(f"{response.json()=}")

    data = response.json()
    assert "processes" in data

    # Verify that there are indeed processes in the list
    assert data["processes"]

    assert response.status_code == 200


@pytest.mark.parametrize(
    ["path", "min_size"],
    [
        ("/", 4),
        ("/file_formats", 2),
        ("/udf_runtimes", 0),
        ("/service_types", 0),
        ("/credentials/oidc", 1),
    ],
)
def test_capabilities_generic(connection, path, min_size):
    """Just check that some generic capability docs return with JSON."""
    response = connection.get(path)
    assert response.status_code == 200
    doc = response.json()
    assert len(doc) >= min_size
