import sys
import datetime
from unittest.mock import MagicMock

# Mock heavy dependencies before importing main
sys.modules['fastf1'] = MagicMock()
sys.modules['quixstreams'] = MagicMock()
sys.modules['quixstreams.models'] = MagicMock()
sys.modules['quixstreams.models.serializers'] = MagicMock()
sys.modules['quixstreams.models.serializers.quix'] = MagicMock()
sys.modules['dotenv'] = MagicMock()

import pytest
import pandas as pd
import numpy as np
from main import timedelta_to_ms, clean_value, row_to_dict


class TestTimedeltaToMs:
    def test_normal_timedelta(self):
        td = pd.Timedelta(seconds=90, milliseconds=123)
        assert timedelta_to_ms(td) == 90123

    def test_nat_returns_none(self):
        assert timedelta_to_ms(pd.NaT) is None

    def test_zero_timedelta(self):
        assert timedelta_to_ms(pd.Timedelta(0)) == 0

    def test_sub_millisecond(self):
        td = pd.Timedelta(microseconds=500)
        assert timedelta_to_ms(td) == 0  # truncated to int ms


class TestCleanValue:
    def test_numpy_int(self):
        val = np.int64(42)
        result = clean_value(val)
        assert result == 42
        assert type(result) is int

    def test_numpy_float(self):
        val = np.float64(3.14)
        result = clean_value(val)
        assert result == pytest.approx(3.14)
        assert type(result) is float

    def test_nan_returns_none(self):
        assert clean_value(float('nan')) is None
        assert clean_value(np.nan) is None

    def test_none_returns_none(self):
        assert clean_value(None) is None

    def test_string_passthrough(self):
        assert clean_value("VER") == "VER"

    def test_timedelta_converted(self):
        td = pd.Timedelta(minutes=1, seconds=30, milliseconds=456)
        assert clean_value(td) == 90456

    def test_datetime_datetime_to_isoformat(self):
        dt = datetime.datetime(2024, 3, 15, 14, 30, 45)
        assert clean_value(dt) == "2024-03-15T14:30:45"

    def test_datetime_timedelta_to_ms(self):
        td = datetime.timedelta(seconds=90, milliseconds=123)
        assert clean_value(td) == 90123

    def test_list_passthrough(self):
        val = [1, 2, 3]
        assert clean_value(val) == [1, 2, 3]

    def test_dict_passthrough(self):
        val = {"key": "value"}
        assert clean_value(val) == {"key": "value"}

    def test_pd_isna_incompatible_types_no_crash(self):
        """Verify that types which cause pd.isna to raise ValueError don't crash."""
        assert clean_value([1, 2]) == [1, 2]
        assert clean_value({"a": 1}) == {"a": 1}
        assert clean_value(np.array([1, 2, 3])) == [1, 2, 3]  # numpy array converted to list

    def test_pd_timestamp_to_isoformat(self):
        ts = pd.Timestamp("2024-03-15 14:30:45")
        assert clean_value(ts) == "2024-03-15T14:30:45"

    def test_datetime_date_to_isoformat(self):
        d = datetime.date(2024, 3, 15)
        assert clean_value(d) == "2024-03-15"


class TestRowToDict:
    def test_basic_row(self):
        row = pd.Series({'Driver': 'VER', 'LapTime': pd.Timedelta(seconds=90), 'Speed': np.float64(320.5)})
        result = row_to_dict(row, ['Driver', 'LapTime', 'Speed'])
        assert result == {'Driver': 'VER', 'LapTime': 90000, 'Speed': pytest.approx(320.5)}

    def test_missing_column_skipped(self):
        row = pd.Series({'Driver': 'HAM'})
        result = row_to_dict(row, ['Driver', 'NonExistent'])
        assert result == {'Driver': 'HAM'}

    def test_nat_becomes_none(self):
        row = pd.Series({'LapTime': pd.NaT})
        result = row_to_dict(row, ['LapTime'])
        assert result == {'LapTime': None}
