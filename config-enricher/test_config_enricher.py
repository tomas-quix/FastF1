"""Unit tests for config-enricher enrich_row logic."""
import pytest
import time
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# Set required env vars before importing main
with patch.dict(os.environ, {
    "CONFIG_TOPIC": "pipeline-config",
    "input_car_data": "openf1-car-data",
    "input_laps": "openf1-laps",
    "input_intervals": "openf1-intervals",
    "input_location": "openf1-location",
    "input_pit": "openf1-pit",
    "input_position": "openf1-position",
    "input_stints": "openf1-stints",
    "input_team_radio": "openf1-team-radio",
    "output_car_data": "enriched-car-data",
    "output_laps": "enriched-laps",
    "output_intervals": "enriched-intervals",
    "output_location": "enriched-location",
    "output_pit": "enriched-pit",
    "output_position": "enriched-position",
    "output_stints": "enriched-stints",
    "output_team_radio": "enriched-team-radio",
    "CONSUMER_GROUP_NAME": "config-enricher-v1",
    "SESSION_KEY": "9161",
    "MEETING_KEY": "1229",
}):
    import main as enricher


DRIVER_CFG = {
    "full_name": "Max Verstappen",
    "team_name": "Red Bull Racing",
    "country_code": "NED",
}
SESSION_CFG = {
    "session_name": "Race",
    "session_type": "Race",
}
MEETING_CFG = {
    "meeting_name": "Bahrain Grand Prix",
    "circuit_short_name": "Bahrain",
}


def _set_store(driver_number=1, session_key="9161", meeting_key="1229",
               driver=None, session=None, meeting=None):
    """Helper to populate the module-level config store."""
    enricher._config_store.clear()
    enricher._last_config_update = time.time()
    if driver is not None:
        enricher._config_store[f"driver/{driver_number}"] = driver
    if session is not None:
        enricher._config_store[f"session/{session_key}"] = session
    if meeting is not None:
        enricher._config_store[f"meeting/{meeting_key}"] = meeting


def teardown_function():
    """Reset store between tests."""
    enricher._config_store.clear()
    enricher._last_config_update = 0.0


# ---------------------------------------------------------------------------
# 1. Full config present
# ---------------------------------------------------------------------------

def test_enrich_with_full_config():
    _set_store(driver=DRIVER_CFG, session=SESSION_CFG, meeting=MEETING_CFG)
    row = {"driver_number": 1, "speed": 300}
    result = enricher.enrich_row(row, driver_number=1, session_key="9161", meeting_key="1229")

    assert result["driver_full_name"] == "Max Verstappen"
    assert result["driver_team_name"] == "Red Bull Racing"
    assert result["driver_country_code"] == "NED"
    assert result["session_name"] == "Race"
    assert result["session_type"] == "Race"
    assert result["meeting_name"] == "Bahrain Grand Prix"
    assert result["circuit_short_name"] == "Bahrain"
    assert "_config_missing" not in result


# ---------------------------------------------------------------------------
# 2. Driver config absent — _config_missing=True, session/meeting still merged
# ---------------------------------------------------------------------------

def test_enrich_missing_driver_config():
    _set_store(session=SESSION_CFG, meeting=MEETING_CFG)
    row = {"driver_number": 99, "speed": 280}
    result = enricher.enrich_row(row, driver_number=99, session_key="9161", meeting_key="1229")

    assert result["_config_missing"] is True
    assert "driver_full_name" not in result
    # Session and meeting should still be enriched
    assert result["session_name"] == "Race"
    assert result["meeting_name"] == "Bahrain Grand Prix"


# ---------------------------------------------------------------------------
# 3. Empty store — _config_missing=True, no crash
# ---------------------------------------------------------------------------

def test_enrich_no_config_at_all():
    enricher._config_store.clear()
    enricher._last_config_update = 0.0
    row = {"driver_number": 1, "speed": 200}
    result = enricher.enrich_row(row, driver_number=1, session_key="9161", meeting_key="1229")

    assert result["_config_missing"] is True
    assert "driver_full_name" not in result
    assert "session_name" not in result
    assert "meeting_name" not in result


# ---------------------------------------------------------------------------
# 4. Original row not mutated
# ---------------------------------------------------------------------------

def test_enrich_does_not_mutate_original():
    _set_store(driver=DRIVER_CFG, session=SESSION_CFG, meeting=MEETING_CFG)
    original = {"driver_number": 1, "speed": 300}
    original_copy = dict(original)
    enricher.enrich_row(original, driver_number=1, session_key="9161", meeting_key="1229")

    assert original == original_copy  # unchanged


# ---------------------------------------------------------------------------
# 5. _config_version and _config_age_ms always present
# ---------------------------------------------------------------------------

def test_config_version_and_age_stamped():
    _set_store(driver=DRIVER_CFG)
    row = {"driver_number": 1}
    result = enricher.enrich_row(row, driver_number=1, session_key="", meeting_key="")

    assert "_config_version" in result
    assert "_config_age_ms" in result
    assert isinstance(result["_config_version"], float)
    assert isinstance(result["_config_age_ms"], int)
    assert result["_config_age_ms"] >= 0
