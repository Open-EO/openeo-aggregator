import os
from pathlib import Path
from unittest import mock

import pytest

from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT, \
    OPENEO_AGGREGATOR_CONFIG, ENVIRONMENT_INDICATOR, get_config

CONFIG_PY_EXAMPLE = """
from openeo_aggregator.config import AggregatorConfig
config = AggregatorConfig(
    config_source=__file__,
    aggregator_backends={"b1": "https://b1.test"},
    streaming_chunk_size=123
)
"""


def test_config_defaults():
    config = AggregatorConfig()
    with pytest.raises(KeyError):
        _ = config.aggregator_backends
    assert config.flask_error_handling is True
    assert config.streaming_chunk_size == STREAM_CHUNK_SIZE_DEFAULT


def test_config_aggregator_backends():
    config = AggregatorConfig(
        aggregator_backends={"b1": "https://b1.test"}
    )
    assert config.aggregator_backends == {"b1": "https://b1.test"}


def test_config_from_py_file(tmp_path):
    path = tmp_path / "aggregator-conf.py"
    with path.open(mode="w") as f:
        f.write(CONFIG_PY_EXAMPLE)
    config = AggregatorConfig.from_py_file(path)
    assert config.config_source == str(path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.streaming_chunk_size == 123


def test_get_config_none_no_env():
    assert OPENEO_AGGREGATOR_CONFIG not in os.environ
    assert ENVIRONMENT_INDICATOR not in os.environ
    config = get_config(None)
    assert config.config_source.endswith("/conf/aggregator.dev.py")


@pytest.mark.parametrize("convertor", [str, Path])
def test_get_config_py_file_path(tmp_path, convertor):
    config_path = tmp_path / "aggregator-conf.py"
    with open(config_path, "w") as f:
        f.write(CONFIG_PY_EXAMPLE)
    config = get_config(convertor(config_path))
    assert config.config_source == str(config_path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.streaming_chunk_size == 123


def test_get_config_env_py_file(tmp_path):
    path = tmp_path / "aggregator-conf.py"
    with path.open(mode="w") as f:
        f.write(CONFIG_PY_EXAMPLE)

    with mock.patch.dict(os.environ, {OPENEO_AGGREGATOR_CONFIG: str(path)}):
        config = get_config(None)
    assert config.config_source == str(path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.streaming_chunk_size == 123


@pytest.mark.parametrize("env", ["dev", "DEV"])
def test_get_config_none_env_dev(env):
    with mock.patch.dict(os.environ, {ENVIRONMENT_INDICATOR: env}):
        config = get_config(None)
    assert config.config_source.endswith("/conf/aggregator.dev.py")


@pytest.mark.parametrize("env", ["prod", "PROD"])
def test_get_config_none_env_prod(env):
    with mock.patch.dict(os.environ, {ENVIRONMENT_INDICATOR: env}):
        config = get_config(None)
    assert config.config_source.endswith("/conf/aggregator.prod.py")
