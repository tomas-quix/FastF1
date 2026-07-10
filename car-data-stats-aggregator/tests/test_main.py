import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("input", "openf1-car-data")
os.environ.setdefault("output", "stats")
os.environ.setdefault("CONSUMER_GROUP", "car-data-stats-aggregator-v1")

from main import build_stats_row


def test_build_stats_row_basic():
    agg_result = {"start": 1733668800000, "end": 1733668860000, "count": 1420}
    row = build_stats_row(agg_result, "81")
    assert row == {
        "driver_number": "81",
        "window_start": 1733668800000,
        "window_end": 1733668860000,
        "count": 1420,
    }


def test_build_stats_row_different_driver_and_window():
    agg_result = {"start": 1733668860000, "end": 1733668920000, "count": 7}
    row = build_stats_row(agg_result, "44")
    assert row == {
        "driver_number": "44",
        "window_start": 1733668860000,
        "window_end": 1733668920000,
        "count": 7,
    }


def test_build_stats_row_no_field_mixup_across_drivers_and_windows():
    row_a = build_stats_row(
        {"start": 1733668800000, "end": 1733668860000, "count": 100}, "1"
    )
    row_b = build_stats_row(
        {"start": 1733668860000, "end": 1733668920000, "count": 200}, "16"
    )

    assert row_a["driver_number"] == "1"
    assert row_a["window_start"] == 1733668800000
    assert row_a["window_end"] == 1733668860000
    assert row_a["count"] == 100

    assert row_b["driver_number"] == "16"
    assert row_b["window_start"] == 1733668860000
    assert row_b["window_end"] == 1733668920000
    assert row_b["count"] == 200

    # Ensure the two results didn't leak into each other.
    assert row_a != row_b
