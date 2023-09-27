import json
import textwrap
from pathlib import Path
from typing import Any

import pytest
from openeo_driver.testing import DictSubSet

from openeo_aggregator.background.prime_caches import AttrStatsProxy, main, prime_caches
from openeo_aggregator.config import AggregatorConfig
from openeo_aggregator.testing import DummyKazooClient

FILE_FORMATS_JUST_GEOTIFF = {
    "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
    "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
}


@pytest.fixture
def config(backend1, backend2, backend1_id, backend2_id, zk_client) -> AggregatorConfig:
    conf = AggregatorConfig()
    conf.aggregator_backends = {
        backend1_id: backend1,
        backend2_id: backend2,
    }
    conf.kazoo_client_factory = lambda **kwargs: zk_client
    conf.zookeeper_prefix = "/oa/"
    conf.memoizer = {
        "type": "zookeeper",
        "config": {
            "zk_hosts": "localhost:2181",
            "default_ttl": 24 * 60 * 60,
        },
    }
    return conf


class TestAttrStatsProxy:
    def test_basic(self):
        class Foo:
            def bar(self, x):
                return x + 1

            def meh(self, x):
                return x * 2

        foo = AttrStatsProxy(target=Foo(), to_track=["bar"])

        assert foo.bar(3) == 4
        assert foo.meh(6) == 12

        assert foo.stats == {"bar": 1}


def test_prime_caches_basic(config, backend1, backend2, requests_mock, mbldr, caplog, zk_client):
    """Just check that bare basics of `prime_caches` work."""
    mocks = [
        requests_mock.get(backend1 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend2 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend1 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend1 + "/collections/S2", json=mbldr.collection("S2")),
        requests_mock.get(backend2 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend2 + "/collections/S2", json=mbldr.collection("S2")),
    ]

    prime_caches(config=config)

    assert all([m.call_count == 1 for m in mocks])

    assert zk_client.get_data_deserialized() == DictSubSet(
        {
            "/oa/cache/CollectionCatalog/all": [
                [DictSubSet({"id": "S2"})],
                DictSubSet({"_jsonserde": DictSubSet()}),
            ],
            "/oa/cache/CollectionCatalog/collection/S2": DictSubSet({"id": "S2"}),
            "/oa/cache/Processing/all/1.1.0": DictSubSet({"load_collection": DictSubSet({"id": "load_collection"})}),
            "/oa/cache/general/file_formats": FILE_FORMATS_JUST_GEOTIFF,
            "/oa/cache/mbcon/api_versions": ["1.1.0"],
            "/oa/cache/SecondaryServices/service_types": {
                "service_types": {},
                "supporting_backend_ids": [],
            },
        }
    )


def _is_primitive_construct(data: Any) -> bool:
    """Consists only of Python primitives int, float, dict, list, str, ...?"""
    if isinstance(data, dict):
        return all(_is_primitive_construct(k) and _is_primitive_construct(v) for k, v in data.items())
    elif isinstance(data, (list, tuple, set)):
        return all(_is_primitive_construct(x) for x in data)
    else:
        return isinstance(data, (bool, int, float, str, bytes)) or data is None


def _get_primitive_config(config: AggregatorConfig) -> dict:
    return {k: v for k, v in config.items() if _is_primitive_construct(v)}


def _build_config_file(config: AggregatorConfig, path: Path):
    """Best effort AggregatorConfig to config file conversion."""
    path.write_text(
        textwrap.dedent(
            f"""
            from openeo_aggregator.config import AggregatorConfig
            config = AggregatorConfig({_get_primitive_config(config)})
            """
        )
    )


def test_prime_caches_main_basic(backend1, backend2, requests_mock, mbldr, caplog, tmp_path, backend1_id, backend2_id):
    """Just check that bare basics of `prime_caches` main work."""
    mocks = [
        requests_mock.get(backend1 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend2 + "/file_formats", json=FILE_FORMATS_JUST_GEOTIFF),
        requests_mock.get(backend1 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend1 + "/collections/S2", json=mbldr.collection("S2")),
        requests_mock.get(backend2 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend2 + "/collections/S2", json=mbldr.collection("S2")),
    ]

    # Construct config file
    config = AggregatorConfig()
    config.aggregator_backends = {
        backend1_id: backend1,
        backend2_id: backend2,
    }
    config_file = tmp_path / "conf.py"
    _build_config_file(config, config_file)

    main(args=["--config", str(config_file)])

    assert all([m.call_count == 1 for m in mocks])


def test_prime_caches_main_logging(backend1, backend2, mbldr, caplog, tmp_path, backend1_id, backend2_id, pytester):
    """Run main in subprocess (so no request mocks, and probably a lot of failures) to see if logging setup works."""

    config = AggregatorConfig()
    config.aggregator_backends = {
        backend1_id: backend1,
        backend2_id: backend2,
    }
    config_file = tmp_path / "conf.py"
    _build_config_file(config, config_file)

    main(args=["--config", str(config_file)])
    log_file = tmp_path / "agg.log"

    result = pytester.run(
        "openeo-aggregator-prime-caches",
        "--config",
        str(config_file),
        "--log-handler",
        "rotating_file_json",
        "--log-file",
        str(log_file),
    )

    assert result.outlines == []
    assert result.errlines == []

    with log_file.open("r") as f:
        log_entries = [json.loads(line) for line in f]

    assert any(log["message"] == f"Loading config from Python file {config_file}" for log in log_entries)
    assert any(log["message"].startswith("Prime caches: start") for log in log_entries)
    assert any(log["message"].startswith("Prime caches: fail") for log in log_entries)
    assert any("cache miss" in log["message"] for log in log_entries)
