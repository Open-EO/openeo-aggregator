import logging

import openeo
import pytest

_log = logging.getLogger(__name__)


def test_openeo_cloud_root_return_sensible_response(connection: openeo.Connection):
    """Check that ${ENDPOINT}/openeo/1.0/ returns something sensible."""
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


@pytest.mark.xfail(reason="Not implemented yet")
def test_collections():
    """Does /collections look ok?"""
    assert False


@pytest.mark.xfail(reason="Not implemented yet")
def test_processes():
    """Does /processes look ok?"""
    assert False
