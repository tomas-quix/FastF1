import sys
import os
from unittest.mock import MagicMock

# Stub out the Quix datalake sink before importing main (requires private PyPI)
mock_sink_module = MagicMock()
mock_sink_module.QuixTSDataLakeSink = MagicMock
sys.modules.setdefault("quixstreams", MagicMock())
sys.modules["quixstreams.sinks"] = MagicMock()
sys.modules["quixstreams.sinks.core"] = MagicMock()
sys.modules["quixstreams.sinks.core.quix_ts_datalake_sink"] = mock_sink_module

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from main import parse_to_ms, prepare


def test_parse_to_ms_known_value():
    result = parse_to_ms("2023-09-03T14:09:04.428000+00:00")
    assert result == 1693750144428


def test_prepare_sets_ts_ms():
    row = {
        "date": "2023-09-03T14:09:04.428000+00:00",
        "year": 2023,
        "circuit": "Monza",
        "session_type": "Race",
        "session_name": "Race",
        "driver_acronym": "STR",
        "lap_number": 31,
    }
    result = prepare(row)
    assert result["ts_ms"] == 1693750144428


def test_prepare_coerces_hive_columns_to_strings():
    row = {
        "date": "2023-09-03T14:09:04.428000+00:00",
        "year": 2023,
        "circuit": "Monza",
        "session_type": "Race",
        "session_name": "Race",
        "driver_acronym": "STR",
        "lap_number": 31,
    }
    result = prepare(row)
    assert isinstance(result["year"], str)
    assert isinstance(result["circuit"], str)
    assert isinstance(result["session_type"], str)
    assert isinstance(result["session_name"], str)
    assert isinstance(result["driver_acronym"], str)
    assert isinstance(result["lap_number"], str)
    assert result["year"] == "2023"
    assert result["lap_number"] == "31"


def test_prepare_lap_number_none_becomes_unknown():
    row = {
        "date": "2023-09-03T14:09:04.428000+00:00",
        "year": 2023,
        "circuit": "Monza",
        "session_type": "Race",
        "session_name": "Race",
        "driver_acronym": "STR",
        "lap_number": None,
    }
    result = prepare(row)
    assert result["lap_number"] == "unknown"
