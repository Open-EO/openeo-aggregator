import datetime

import pytest

from openeo_aggregator.testing import clock_mock, approx_now
from openeo_aggregator.utils import Clock


@pytest.mark.parametrize("fail", [True, False])
def test_mock_clock_basic(fail):
    assert Clock.time() > 1600000000
    assert Clock.utcnow().year > 2020
    try:
        with clock_mock():
            assert Clock.time() == 1500000000
            assert Clock.utcnow() == datetime.datetime(2017, 7, 14, 2, 40, 0)
            if fail:
                raise RuntimeError
    except RuntimeError:
        pass
    assert Clock.time() > 1600000000
    assert Clock.utcnow().year > 2020


@pytest.mark.parametrize(["start", "expected_time", "expected_date"], [
    (1000, 1000, datetime.datetime(1970, 1, 1, 0, 16, 40)),
    ("2021-02-21", 1613865600, datetime.datetime(2021, 2, 21)),
    ("2021-02-21T12:34:56Z", 1613910896, datetime.datetime(2021, 2, 21, 12, 34, 56)),
])
def test_mock_clock_start(start, expected_time, expected_date):
    assert Clock.time() == approx_now()
    with clock_mock(start=start):
        assert Clock.time() == expected_time
        assert Clock.utcnow() == expected_date
    assert Clock.time() == approx_now()


@pytest.mark.parametrize("step", [1, 2, .1])
def test_clock_mock_step(step):
    with clock_mock(start=1000, step=step):
        assert Clock.time() == 1000
        assert Clock.time() == 1000 + step
        assert Clock.time() == 1000 + 2 * step
