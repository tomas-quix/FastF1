import sys
from unittest.mock import MagicMock

# Mock heavy dependencies before importing main
sys.modules["fastf1"] = MagicMock()
sys.modules["quixstreams"] = MagicMock()

import numpy as np
import pandas as pd
import pytest

from main import safe_value


class TestSafeValueNaN:
    def test_float_nan_returns_none(self):
        assert safe_value(float("nan")) is None

    def test_numpy_nan_returns_none(self):
        assert safe_value(np.nan) is None

    def test_pandas_na_returns_none(self):
        assert safe_value(pd.NA) is None


class TestSafeValueNaT:
    def test_nat_returns_none(self):
        assert safe_value(pd.NaT) is None


class TestSafeValueTimedelta:
    def test_timedelta_to_milliseconds(self):
        td = pd.Timedelta(minutes=1, seconds=30, milliseconds=123)
        result = safe_value(td)
        assert result == pytest.approx(90123.0)

    def test_zero_timedelta(self):
        td = pd.Timedelta(0)
        assert safe_value(td) == 0.0

    def test_timedelta_seconds_only(self):
        td = pd.Timedelta(seconds=45)
        assert safe_value(td) == pytest.approx(45000.0)

    def test_timedelta_sub_millisecond(self):
        td = pd.Timedelta(microseconds=500)
        assert safe_value(td) == pytest.approx(0.5)


class TestSafeValueTimestamp:
    def test_timestamp_to_isoformat(self):
        ts = pd.Timestamp("2024-05-26T14:00:00")
        result = safe_value(ts)
        assert result == "2024-05-26T14:00:00"

    def test_timestamp_with_tz(self):
        ts = pd.Timestamp("2024-05-26T14:00:00", tz="UTC")
        result = safe_value(ts)
        assert "2024-05-26T14:00:00" in result


class TestSafeValueNumpyTypes:
    def test_numpy_int64_to_int(self):
        val = np.int64(42)
        result = safe_value(val)
        assert result == 42
        assert isinstance(result, int)

    def test_numpy_int32_to_int(self):
        val = np.int32(7)
        result = safe_value(val)
        assert result == 7
        assert isinstance(result, int)

    def test_numpy_float64_to_float(self):
        val = np.float64(3.14)
        result = safe_value(val)
        assert result == pytest.approx(3.14)
        assert isinstance(result, float)

    def test_numpy_float32_to_float(self):
        val = np.float32(2.5)
        result = safe_value(val)
        assert result == pytest.approx(2.5)
        assert isinstance(result, float)

    def test_numpy_bool_true(self):
        val = np.bool_(True)
        result = safe_value(val)
        assert result is True
        assert isinstance(result, bool)

    def test_numpy_bool_false(self):
        val = np.bool_(False)
        result = safe_value(val)
        assert result is False
        assert isinstance(result, bool)


class TestSafeValuePassthrough:
    def test_string_passthrough(self):
        assert safe_value("hello") == "hello"

    def test_int_passthrough(self):
        assert safe_value(42) == 42

    def test_float_passthrough(self):
        assert safe_value(3.14) == pytest.approx(3.14)

    def test_bool_passthrough(self):
        assert safe_value(True) is True

    def test_none_returns_none(self):
        assert safe_value(None) is None

    def test_list_passthrough(self):
        val = [1, 2, 3]
        assert safe_value(val) == [1, 2, 3]
