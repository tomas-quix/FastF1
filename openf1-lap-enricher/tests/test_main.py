import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from main import parse_to_ms, car_data_ts, lap_ts


def test_parse_to_ms_basic():
    ms = parse_to_ms("2023-09-03T14:42:50.273000+00:00")
    # 2023-09-03T14:42:50.273 UTC
    assert ms == 1693752170273


def test_car_data_ts_extracts_date():
    ms = car_data_ts({"date": "2023-09-03T14:42:50.273000+00:00"}, [], 0, 0)
    assert ms == 1693752170273


def test_lap_ts_extracts_date_start():
    ms = lap_ts({"date_start": "2023-09-03T14:43:31.982000+00:00"}, [], 0, 0)
    assert ms == 1693752211982


def test_car_data_ts_fallback():
    ms = car_data_ts({}, [], 99999, 0)
    assert ms == 99999


def test_lap_ts_fallback():
    ms = lap_ts({}, [], 88888, 0)
    assert ms == 88888
