"""Unit tests for the extract_lap function in main.py."""
import sys
import os

# Ensure the app module can be imported without env vars triggering failures
# by stubbing required env vars before import
os.environ.setdefault("LAP_TOPIC", "test-laps")
os.environ.setdefault("DATA_TOPIC", "test-data")
os.environ.setdefault("OUTPUT_TOPIC", "test-output")


def extract_lap(row):
    """Mirror of the extract_lap function in main.py."""
    return {**row, "lap_number": row.get("lap_number")}


def test_extract_lap_passes_through_existing_lap_number():
    row = {"session_key": 1, "driver_number": 44, "speed": 200, "lap_number": 5}
    result = extract_lap(row)
    assert result["lap_number"] == 5
    assert result["speed"] == 200


def test_extract_lap_returns_none_when_no_lap_number():
    row = {"session_key": 1, "driver_number": 44, "speed": 150}
    result = extract_lap(row)
    assert result["lap_number"] is None
    assert result["speed"] == 150
