import os
from pathlib import Path
from unittest import mock

import pytest

from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT, OPENEO_AGGREGATOR_CONFIG
from openeo_aggregator.config import DEFAULT_CONFIG, get_config

CONFIG_JSON_EXAMPLE = '{"aggregator_backends":{"b1":"https://b1.test"},"streaming_chunk_size":123}'


def test_config_defaults():
    config = AggregatorConfig()
    with pytest.raises(KeyError):
        _ = config.aggregator_backends
    assert config.auto_logging_setup is True
    assert config.flask_error_handling is True
    assert config.streaming_chunk_size == STREAM_CHUNK_SIZE_DEFAULT


def test_config_aggregator_backends():
    config = AggregatorConfig(
        aggregator_backends={"b1": "https://b1.test"}
    )
    assert config.aggregator_backends == {"b1": "https://b1.test"}


def test_config_from_json():
    data = '{"aggregator_backends": {"b1": "https://b1.test"}, "streaming_chunk_size": 123}'
    config = AggregatorConfig.from_json(data)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.streaming_chunk_size == 123


def test_config_from_json_file(tmp_path):
    config_path = tmp_path / "config.json"
    with config_path.open("w") as f:
        f.write('{"aggregator_backends": {"b1": "https://b1.test"}, "streaming_chunk_size": 123}')
    config = AggregatorConfig.from_json_file(str(config_path))
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.streaming_chunk_size == 123


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


def test_get_config_json_url_encoded():
    # from: `import urllib.parse; urllib.parse.quote('{"aggregator_backends":{"b1":"https://b1.test"},"streaming_chunk_size":123}')`
    data = '%7B%22aggregator_backends%22%3A%7B%22b1%22%3A%22https%3A//b1.test%22%7D%2C%22streaming_chunk_size%22%3A123%7D'
    config = get_config(data)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.auto_logging_setup is True
    assert config.streaming_chunk_size == 123
