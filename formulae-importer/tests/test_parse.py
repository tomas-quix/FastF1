import json
import os
import sys
import unittest
import unittest.mock as mock

# Patch quixstreams before importing main to avoid needing a real broker
sys.modules.setdefault("quixstreams", mock.MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as m

FIXTURE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "fixtures", "2025_berlin_eprix_response.json"
)


def _load_fixture_html() -> str:
    with open(FIXTURE_PATH) as f:
        data = json.load(f)
    return data["parse"]["text"]["*"]


class TestParseHtmlToRows(unittest.TestCase):

    EXPECTED_KEYS = {
        "season", "event_name", "race_number", "session_type", "position",
        "car_number", "driver_name", "driver_nationality", "team", "laps",
        "time_or_gap", "grid", "points", "ts_ms",
    }

    @classmethod
    def setUpClass(cls):
        cls.html = _load_fixture_html()
        cls.rows = m.parse_html_to_rows(cls.html, "2025 Berlin ePrix", "2024-25")

    def test_rows_returned(self):
        self.assertGreater(len(self.rows), 0)

    def test_expected_keys_present_in_every_row(self):
        for row in self.rows:
            self.assertEqual(set(row.keys()), self.EXPECTED_KEYS)

    def test_field_types(self):
        for row in self.rows:
            self.assertIsInstance(row["position"], int)
            self.assertIsInstance(row["car_number"], int)
            self.assertIsInstance(row["laps"], int)
            self.assertIsInstance(row["grid"], int)
            self.assertIsInstance(row["points"], float)
            self.assertIsInstance(row["ts_ms"], int)
            self.assertGreater(row["ts_ms"], 0)

    def test_at_least_two_race_sessions_found(self):
        """Berlin is a doubleheader — expect Race #1 and Race #2 classifications."""
        race_numbers = {row["race_number"] for row in self.rows if row["session_type"] == "Race"}
        self.assertEqual(race_numbers, {1, 2})

    def test_race_one_has_at_least_20_rows(self):
        race_one_rows = [r for r in self.rows if r["session_type"] == "Race" and r["race_number"] == 1]
        self.assertGreaterEqual(len(race_one_rows), 20)

    def test_qualifying_sessions_found(self):
        quali_numbers = {row["race_number"] for row in self.rows if row["session_type"] == "Qualifying"}
        self.assertEqual(quali_numbers, {1, 2})

    def test_footer_source_row_excluded(self):
        for row in self.rows:
            self.assertNotIn("Source", row["driver_name"])

    def test_winner_row_values(self):
        winner = next(
            r for r in self.rows
            if r["session_type"] == "Race" and r["race_number"] == 1 and r["position"] == 1
        )
        self.assertEqual(winner["driver_name"], "Mitch Evans")
        self.assertEqual(winner["team"], "Jaguar")
        self.assertEqual(winner["car_number"], 9)
        self.assertEqual(winner["laps"], 41)
        self.assertEqual(winner["time_or_gap"], "49:54.398")
        self.assertEqual(winner["grid"], 1)
        self.assertEqual(winner["points"], 25.0)
        self.assertEqual(winner["driver_nationality"], "NZL")
        self.assertEqual(winner["season"], "2024-25")
        self.assertEqual(winner["event_name"], "2025 Berlin ePrix")

    def test_bonus_point_footnote_is_stripped_to_base_points(self):
        """'18+1<footnote>' renders as '18+11' — base points must resolve to 18.0."""
        second = next(
            r for r in self.rows
            if r["session_type"] == "Race" and r["race_number"] == 1 and r["position"] == 2
        )
        self.assertEqual(second["points"], 18.0)

    def test_grid_footnote_marker_is_stripped(self):
        """Grid values like '9[c]' must resolve to a clean int."""
        for row in self.rows:
            self.assertGreaterEqual(row["grid"], 0)


if __name__ == "__main__":
    unittest.main()
