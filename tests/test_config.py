import os
import textwrap
from pathlib import Path
from unittest import mock

import pytest

from openeo_aggregator.config import (
    OPENEO_AGGREGATOR_CONFIG,
    STREAM_CHUNK_SIZE_DEFAULT,
    AggregatorConfig,
    ConfigException,
    get_config,
)


def _get_config_content(config_var_name: str = "config"):
    return textwrap.dedent(
        f"""
        from openeo_aggregator.config import AggregatorConfig
        {config_var_name} = AggregatorConfig(
            config_source=__file__,
            aggregator_backends={{"b1": "https://b1.test"}},
            test_dummy="bob",
        )
        """
    )


def test_config_defaults():
    config = AggregatorConfig()
    with pytest.raises(KeyError):
        _ = config.aggregator_backends
    assert config.test_dummy == "alice"


def test_config_aggregator_backends():
    config = AggregatorConfig(
        aggregator_backends={"b1": "https://b1.test"}
    )
    assert config.aggregator_backends == {"b1": "https://b1.test"}


@pytest.mark.parametrize("config_var_name", ["aggregator_config", "config"])
def test_config_from_py_file(tmp_path, config_var_name):
    path = tmp_path / "aggregator-conf.py"
    path.write_text(_get_config_content(config_var_name=config_var_name))
    config = AggregatorConfig.from_py_file(path)
    assert config.config_source == str(path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.test_dummy == "bob"


def test_config_from_py_file_wrong_config_var_name(tmp_path):
    path = tmp_path / "aggregator-conf.py"
    path.write_text(_get_config_content(config_var_name="meh"))
    with pytest.raises(ConfigException, match="No 'config' variable defined in config file"):
        AggregatorConfig.from_py_file(path)


def test_get_config_default_no_env():
    assert OPENEO_AGGREGATOR_CONFIG not in os.environ
    config = get_config()
    assert config.config_source.endswith("/conf/aggregator.dummy.py")


@pytest.mark.parametrize("convertor", [str, Path])
def test_get_config_py_file_path(tmp_path, convertor):
    config_path = tmp_path / "aggregator-conf.py"
    config_path.write_text(_get_config_content())
    config = get_config(convertor(config_path))
    assert config.config_source == str(config_path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.test_dummy == "bob"


def test_get_config_env_py_file(tmp_path):
    path = tmp_path / "aggregator-conf.py"
    path.write_text(_get_config_content())

    with mock.patch.dict(os.environ, {OPENEO_AGGREGATOR_CONFIG: str(path)}):
        config = get_config()
    assert config.config_source == str(path)
    assert config.aggregator_backends == {"b1": "https://b1.test"}
    assert config.test_dummy == "bob"
