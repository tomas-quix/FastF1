"""
Unit tests for lap_join helper module.

Each test clears the shared lap_index dict so tests are fully isolated.
"""
import pytest
import lap_join


@pytest.fixture(autouse=True)
def clear_lap_index():
    """Reset the global lap_index before every test."""
    lap_join.lap_index.clear()
    yield
    lap_join.lap_index.clear()


SESSION = 9157
DRIVER = 55


def _add_lap(session_key, driver_number, lap_number, date_start):
    """Helper to insert a lap record via process_lap."""
    lap_join.process_lap(
        {
            "session_key": session_key,
            "driver_number": driver_number,
            "lap_number": lap_number,
            "date_start": date_start,
        }
    )


# ---------------------------------------------------------------------------
# 1. Returns correct lap for a timestamp between two lap starts
# ---------------------------------------------------------------------------
def test_returns_correct_lap_between_two_starts():
    _add_lap(SESSION, DRIVER, 1, "2023-09-03T14:40:00+00:00")
    _add_lap(SESSION, DRIVER, 2, "2023-09-03T14:42:00+00:00")
    _add_lap(SESSION, DRIVER, 3, "2023-09-03T14:44:00+00:00")

    # Timestamp between lap 2 start (14:42) and lap 3 start (14:44)
    result = lap_join.get_lap_number(SESSION, DRIVER, "2023-09-03T14:43:00+00:00")
    assert result == 2, f"Expected lap 2, got {result}"


# ---------------------------------------------------------------------------
# 2. Returns None when no laps are loaded for the driver
# ---------------------------------------------------------------------------
def test_returns_none_when_no_laps_loaded():
    # Nothing in lap_index for this driver
    result = lap_join.get_lap_number(SESSION, DRIVER, "2023-09-03T14:43:00+00:00")
    assert result is None


# ---------------------------------------------------------------------------
# 3. Returns the first lap when timestamp is before any lap start
# ---------------------------------------------------------------------------
def test_returns_first_lap_when_before_any_start():
    _add_lap(SESSION, DRIVER, 1, "2023-09-03T14:40:00+00:00")
    _add_lap(SESSION, DRIVER, 2, "2023-09-03T14:42:00+00:00")

    # Timestamp before lap 1 starts
    result = lap_join.get_lap_number(SESSION, DRIVER, "2023-09-03T14:39:00+00:00")
    assert result == 1, f"Expected first lap (1), got {result}"


# ---------------------------------------------------------------------------
# 4. Boundary: timestamp exactly at date_start returns that lap
# ---------------------------------------------------------------------------
def test_boundary_exact_date_start_returns_that_lap():
    _add_lap(SESSION, DRIVER, 1, "2023-09-03T14:40:00+00:00")
    _add_lap(SESSION, DRIVER, 2, "2023-09-03T14:42:00+00:00")

    # Exactly at lap 2's start time
    result = lap_join.get_lap_number(SESSION, DRIVER, "2023-09-03T14:42:00+00:00")
    assert result == 2, f"Expected lap 2 at exact boundary, got {result}"
