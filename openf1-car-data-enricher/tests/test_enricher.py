"""Unit tests for openf1-car-data-enricher/main.py."""
import sys
import os

# Allow importing main.py from the parent directory without a package setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from main import build_lookup_key, enrich_message, parse_lookup_fields


# ---------------------------------------------------------------------------
# parse_lookup_fields
# ---------------------------------------------------------------------------

def test_parse_lookup_fields_basic():
    raw = '{"driver_full_name":"drivers.full_name","year":"sessions.year"}'
    fields = parse_lookup_fields(raw)
    assert ("driver_full_name", "drivers", "full_name") in fields
    assert ("year", "sessions", "year") in fields


def test_parse_lookup_fields_full_default():
    raw = (
        '{"driver_full_name":"drivers.full_name",'
        '"year":"sessions.year",'
        '"circuit":"sessions.circuit_short_name",'
        '"session_type":"sessions.session_type",'
        '"session_name":"sessions.session_name",'
        '"meeting_name":"meetings.meeting_name"}'
    )
    fields = parse_lookup_fields(raw)
    assert len(fields) == 6
    categories = {cat for _, cat, _ in fields}
    assert categories == {"drivers", "sessions", "meetings"}


# ---------------------------------------------------------------------------
# build_lookup_key
# ---------------------------------------------------------------------------

def test_build_lookup_key_drivers():
    value = {"meeting_key": 1218, "session_key": 9157, "driver_number": 55}
    assert build_lookup_key("drivers", value) == "1218-9157-55"


def test_build_lookup_key_sessions():
    value = {"meeting_key": 1218, "session_key": 9157, "driver_number": 55}
    assert build_lookup_key("sessions", value) == "1218-9157"


def test_build_lookup_key_meetings():
    value = {"meeting_key": 1218, "session_key": 9157, "driver_number": 55}
    assert build_lookup_key("meetings", value) == "1218"


def test_build_lookup_key_unknown_category():
    value = {"meeting_key": 1218, "session_key": 9157, "driver_number": 55}
    assert build_lookup_key("unknown", value) == ""


# ---------------------------------------------------------------------------
# enrich_message
# ---------------------------------------------------------------------------

SAMPLE_CAR_DATA = {
    "date": "2023-09-03T14:42:49.593000+00:00",
    "session_key": 9157,
    "n_gear": 0,
    "meeting_key": 1218,
    "driver_number": 55,
    "drs": 0,
    "rpm": 0,
    "brake": 0,
    "speed": 0,
    "throttle": 0,
}

SAMPLE_LOOKUP_FIELDS = [
    ("driver_full_name", "drivers", "full_name"),
    ("year", "sessions", "year"),
    ("circuit", "sessions", "circuit_short_name"),
    ("session_type", "sessions", "session_type"),
    ("session_name", "sessions", "session_name"),
    ("meeting_name", "meetings", "meeting_name"),
]

SAMPLE_CACHE = {
    ("drivers", "1218-9157-55"): {
        "full_name": "Carlos Sainz",
        "name_acronym": "SAI",
        "driver_number": 55,
    },
    ("sessions", "1218-9157"): {
        "year": 2023,
        "circuit_short_name": "Monza",
        "session_type": "Race",
        "session_name": "Race",
        "country_name": "Italy",
    },
    ("meetings", "1218"): {
        "meeting_name": "Italian Grand Prix",
        "country_name": "Italy",
    },
}


class TestHappyPath:
    """All three config entries present — all six fields must be enriched."""

    def test_all_enriched_fields_present(self):
        result = enrich_message(SAMPLE_CAR_DATA, SAMPLE_CACHE, SAMPLE_LOOKUP_FIELDS)
        assert result["driver_full_name"] == "Carlos Sainz"
        assert result["year"] == 2023
        assert result["circuit"] == "Monza"
        assert result["session_type"] == "Race"
        assert result["session_name"] == "Race"
        assert result["meeting_name"] == "Italian Grand Prix"

    def test_original_telemetry_fields_preserved(self):
        result = enrich_message(SAMPLE_CAR_DATA, SAMPLE_CACHE, SAMPLE_LOOKUP_FIELDS)
        for key in SAMPLE_CAR_DATA:
            assert key in result
            assert result[key] == SAMPLE_CAR_DATA[key]

    def test_original_message_not_mutated(self):
        original = {**SAMPLE_CAR_DATA}
        enrich_message(SAMPLE_CAR_DATA, SAMPLE_CACHE, SAMPLE_LOOKUP_FIELDS)
        assert SAMPLE_CAR_DATA == original


class TestCacheMiss:
    """Empty cache — message must pass through unchanged without crashing."""

    def test_message_passes_through_on_empty_cache(self):
        empty_cache = {}
        result = enrich_message(SAMPLE_CAR_DATA, empty_cache, SAMPLE_LOOKUP_FIELDS)
        # No exception raised
        assert result is not None

    def test_no_enrichment_fields_added_on_empty_cache(self):
        empty_cache = {}
        result = enrich_message(SAMPLE_CAR_DATA, empty_cache, SAMPLE_LOOKUP_FIELDS)
        enrichment_keys = {field for field, _, _ in SAMPLE_LOOKUP_FIELDS}
        for key in enrichment_keys:
            assert key not in result

    def test_original_fields_still_present_on_empty_cache(self):
        empty_cache = {}
        result = enrich_message(SAMPLE_CAR_DATA, empty_cache, SAMPLE_LOOKUP_FIELDS)
        for key in SAMPLE_CAR_DATA:
            assert key in result


class TestPartialCache:
    """Driver entry present, session and meeting missing."""

    def _partial_cache(self):
        return {("drivers", "1218-9157-55"): SAMPLE_CACHE[("drivers", "1218-9157-55")]}

    def test_driver_full_name_enriched(self):
        result = enrich_message(SAMPLE_CAR_DATA, self._partial_cache(), SAMPLE_LOOKUP_FIELDS)
        assert result["driver_full_name"] == "Carlos Sainz"

    def test_session_fields_absent(self):
        result = enrich_message(SAMPLE_CAR_DATA, self._partial_cache(), SAMPLE_LOOKUP_FIELDS)
        assert "year" not in result
        assert "circuit" not in result
        assert "session_type" not in result
        assert "session_name" not in result

    def test_meeting_field_absent(self):
        result = enrich_message(SAMPLE_CAR_DATA, self._partial_cache(), SAMPLE_LOOKUP_FIELDS)
        assert "meeting_name" not in result

    def test_original_fields_preserved(self):
        result = enrich_message(SAMPLE_CAR_DATA, self._partial_cache(), SAMPLE_LOOKUP_FIELDS)
        for key in SAMPLE_CAR_DATA:
            assert key in result


class TestEdgeCases:
    """Miscellaneous edge cases."""

    def test_empty_lookup_fields(self):
        result = enrich_message(SAMPLE_CAR_DATA, SAMPLE_CACHE, [])
        assert result == SAMPLE_CAR_DATA

    def test_missing_source_field_in_content(self):
        """If the source field doesn't exist in the fetched content, the output field is absent."""
        cache = {("drivers", "1218-9157-55"): {"name_acronym": "SAI"}}  # no full_name
        fields = [("driver_full_name", "drivers", "full_name")]
        result = enrich_message(SAMPLE_CAR_DATA, cache, fields)
        assert "driver_full_name" not in result

    def test_none_meeting_key_handled(self):
        """Messages with missing key fields produce empty target_key → cache lookup skipped."""
        value = {**SAMPLE_CAR_DATA}
        del value["meeting_key"]
        result = enrich_message(value, SAMPLE_CACHE, SAMPLE_LOOKUP_FIELDS)
        # meeting config lookup key becomes "" — should not crash
        assert result is not None
