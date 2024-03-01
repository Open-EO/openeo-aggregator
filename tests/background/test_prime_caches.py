import json
import logging
import re
from unittest import mock

import pytest
from openeo_driver.testing import DictSubSet

import openeo_aggregator.caching
from openeo_aggregator.background.prime_caches import main, prime_caches
from openeo_aggregator.testing import config_overrides

FILE_FORMATS_JUST_GEOTIFF = {
    "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
    "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
}


@pytest.fixture(autouse=True)
def _use_zookeeper_memoizer():
    with config_overrides(
        memoizer={
            "type": "zookeeper",
            "config": {
                "zk_hosts": "zk.test:2181",
                "default_ttl": 24 * 60 * 60,
            },
        }
    ):
        yield


@pytest.fixture(autouse=True)
def _mock_kazoo_client(zk_client):
    with mock.patch.object(openeo_aggregator.caching, "KazooClient", return_value=zk_client):
        yield


@pytest.fixture
def upstream_request_mocks(requests_mock, backend1, backend2, mbldr) -> list:
    return [
        requests_mock.get(backend1 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend2 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend1 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend1 + "/collections/S2", json=mbldr.collection("S2")),
        requests_mock.get(backend2 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend2 + "/collections/S2", json=mbldr.collection("S2")),
    ]


def test_prime_caches_basic(upstream_request_mocks, zk_client):
    """Just check that bare basics of `prime_caches` work."""

    prime_caches()

    assert all([m.call_count == 1 for m in upstream_request_mocks])

    assert zk_client.get_data_deserialized() == DictSubSet(
        {
            "/o-a/cache/CollectionCatalog/all": [
                [DictSubSet({"id": "S2"})],
                DictSubSet({"_jsonserde": DictSubSet()}),
            ],
            "/o-a/cache/CollectionCatalog/collection/S2": DictSubSet({"id": "S2"}),
            "/o-a/cache/Processing/all/1.1.0": DictSubSet({"load_collection": DictSubSet({"id": "load_collection"})}),
            "/o-a/cache/general/file_formats": FILE_FORMATS_JUST_GEOTIFF,
            "/o-a/cache/mbcon/api_versions": ["1.1.0"],
            "/o-a/cache/SecondaryServices/service_types": {
                "service_types": {},
                "supporting_backend_ids": [],
            },
        }
    )


@pytest.mark.parametrize("zk_memoizer_tracking", [False, True])
def test_prime_caches_stats(upstream_request_mocks, caplog, zk_client, zk_memoizer_tracking):
    """Check logging of Zookeeper operation stats."""
    caplog.set_level(logging.INFO)
    with config_overrides(zk_memoizer_tracking=zk_memoizer_tracking):
        prime_caches()

    assert all([m.call_count == 1 for m in upstream_request_mocks])

    (zk_stats,) = [r.message for r in caplog.records if r.message.startswith("ZooKeeper stats:")]
    if zk_memoizer_tracking:
        assert re.search(r"kazoo_stats=\{.*start.*create.*\} zk_writes=[1-9]\d*", zk_stats)
    else:
        assert zk_stats == "ZooKeeper stats: not configured"


def test_prime_caches_main_basic(backend1, backend2, upstream_request_mocks, tmp_path, backend1_id, backend2_id):
    """Just check that bare basics of `prime_caches` main work."""
    main(args=[])
    assert all([m.call_count == 1 for m in upstream_request_mocks])


def test_prime_caches_main_logging(backend1, backend2, tmp_path, backend1_id, backend2_id, pytester):
    """Run main in subprocess (so no request mocks, and probably a lot of failures) to see if logging setup works."""

    log_file = tmp_path / "agg.log"

    result = pytester.run(
        "openeo-aggregator-prime-caches",
        "--log-handler",
        "rotating_file_json",
        "--log-file",
        str(log_file),
    )

    assert result.outlines == []
    assert result.errlines == []

    with log_file.open("r") as f:
        log_entries = [json.loads(line) for line in f]
    assert len(log_entries) > 0

    assert any(
        re.match("Loaded config config_id='aggregator-dummy' from.*/tests/backend_config.py", log["message"])
        for log in log_entries
    )
    assert any(log["message"].startswith("Prime caches: start") for log in log_entries)
    assert any(log["message"].startswith("Prime caches: fail") for log in log_entries)
    assert any("cache miss" in log["message"] for log in log_entries)
