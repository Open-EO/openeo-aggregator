import pytest

from openeo_aggregator.config import AggregatorConfig, STREAM_CHUNK_SIZE_DEFAULT


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
