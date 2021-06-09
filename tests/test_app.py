import os
from pathlib import Path
from unittest import mock

import pytest

from openeo_aggregator.app import get_config, OPENEO_AGGREGATOR_CONFIG
from openeo_aggregator.config import DEFAULT_CONFIG

CONFIG_JSON_EXAMPLE = '{"aggregator_backends":{"b1":"https://b1.test"},"streaming_chunk_size":123}'


def test_get_config_none_no_env():
    assert OPENEO_AGGREGATOR_CONFIG not in os.environ
    config = get_config(None)
    assert config is DEFAULT_CONFIG


def test_get_config_json_str():
    config = get_config(CONFIG_JSON_EXAMPLE)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.auto_logging_setup is True
    assert config.streaming_chunk_size == 123


@pytest.mark.parametrize("convertor", [str, Path])
def test_get_config_json_path(tmp_path, convertor):
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        f.write(CONFIG_JSON_EXAMPLE)
    config = get_config(convertor(config_path))
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.auto_logging_setup is True
    assert config.streaming_chunk_size == 123


def test_get_config_none_env_json_str():
    with mock.patch.dict(os.environ, {OPENEO_AGGREGATOR_CONFIG: CONFIG_JSON_EXAMPLE}):
        config = get_config(None)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.auto_logging_setup is True
    assert config.streaming_chunk_size == 123


def test_get_config_none_env_json_path(tmp_path):
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        f.write(CONFIG_JSON_EXAMPLE)
    with mock.patch.dict(os.environ, {OPENEO_AGGREGATOR_CONFIG: str(config_path)}):
        config = get_config(None)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.auto_logging_setup is True
    assert config.streaming_chunk_size == 123
