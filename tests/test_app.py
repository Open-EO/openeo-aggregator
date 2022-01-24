import flask
import pytest

from openeo_aggregator.app import create_app
from openeo_aggregator.config import AggregatorConfig
from .conftest import get_api100, get_flask_app


def test_create_app(config: AggregatorConfig):
    app = create_app(config)
    assert isinstance(app, flask.Flask)


@pytest.mark.parametrize(["partitioned_job_tracking", "expected"], [
    (None, False),
    ({"zk_client": "dummy"}, True),
])
def test_create_app_no_partitioned_job_tracking(config: AggregatorConfig, partitioned_job_tracking, expected):
    config.partitioned_job_tracking = partitioned_job_tracking
    api100 = get_api100(get_flask_app(config))
    res = api100.get("/").assert_status_code(200).json
    assert res["_partitioned_job_tracking"] is expected
