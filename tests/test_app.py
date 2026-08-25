import flask
import pytest

from openeo_aggregator.app import create_app
from openeo_aggregator.testing import config_overrides


def test_create_app():
    app = create_app()
    assert isinstance(app, flask.Flask)


@pytest.mark.parametrize(
    ["partitioned_job_tracking", "expected"],
    [
        (None, False),
        ({"zk_client": "dummy"}, True),
    ],
)
def test_create_app_no_partitioned_job_tracking(get_api, partitioned_job_tracking, expected):
    with config_overrides(partitioned_job_tracking=partitioned_job_tracking):
        api = get_api()
        res = api.get("/").assert_status_code(200).json
        assert res["_partitioned_job_tracking"] is expected
