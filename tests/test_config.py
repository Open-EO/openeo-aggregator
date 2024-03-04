import pytest

from openeo_aggregator.config import AggregatorBackendConfig


def test_config_defaults():
    with pytest.raises(TypeError, match="missing.*required.*aggregator_backends"):
        _ = AggregatorBackendConfig()


def test_config_aggregator_backends_empty():
    with pytest.raises(ValueError, match="Length of 'aggregator_backends' must be >= 1"):
        _ = AggregatorBackendConfig(aggregator_backends={})


def test_config_aggregator_backends():
    config = AggregatorBackendConfig(aggregator_backends={"b1": "https://b1.test"})
    assert config.aggregator_backends == {"b1": "https://b1.test"}
