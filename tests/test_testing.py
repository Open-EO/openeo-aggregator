import datetime

import pytest

from openeo_aggregator.testing import (
    clock_mock,
    approx_now,
    approx_str_prefix,
    approx_str_contains,
    approx_str_suffix,
    same_repr,
)
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


def test_clock_mock_offset():
    with clock_mock(start=10000):
        assert Clock.time() == 10000
        with clock_mock(offset=11):
            assert Clock.time() == 10011
        assert Clock.time() == 10000


def test_approx_str_prefix():
    assert "foobar" == approx_str_prefix("foo")
    assert "foobar" != approx_str_prefix("bar")
    assert not ("foobar" == approx_str_prefix("bar"))


def test_approx_str_suffix():
    assert "foobar" == approx_str_suffix("bar")
    assert "foobar" != approx_str_suffix("foo")
    assert not ("foobar" == approx_str_suffix("foo"))


def test_approx_str_contains():
    assert "foobar" == approx_str_contains("foo")
    assert "foobar" == approx_str_contains("bar")
    assert "foobar" == approx_str_contains("oba")
    assert "foobar" != approx_str_contains("meh")
    assert not ("foobar" == approx_str_contains("meh"))


def test_approx_str_nesting():
    actual = {"id": 123, "msg": "error: lookup failure #845 (confirmed)"}
    assert actual == {"id": 123, "msg": approx_str_prefix("error:")}
    assert actual == {"id": 123, "msg": approx_str_contains("lookup failure")}
    assert actual == {"id": 123, "msg": approx_str_suffix("(confirmed)")}


def test_same_repr():
    assert TypeError("foo") == same_repr(TypeError("foo"))
    assert float("nan") == same_repr(float("nan"))
    assert 123 != same_repr("123")
