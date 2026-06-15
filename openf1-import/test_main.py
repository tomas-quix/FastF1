import unittest
from unittest.mock import patch, MagicMock, call
import json

from main import (
    resolve_session_key,
    fetch_endpoint,
    produce_records,
    ENDPOINTS,
)


class TestResolveSessionKey(unittest.TestCase):
    """Tests for resolve_session_key (two-step: meetings -> sessions)."""

    @patch("main.requests.get")
    def test_happy_path(self, mock_get):
        """Single meeting + single session — should return session_key."""
        mock_meetings_resp = MagicMock()
        mock_meetings_resp.json.return_value = [
            {"meeting_key": 1229, "meeting_name": "Bahrain Grand Prix"}
        ]
        mock_meetings_resp.raise_for_status = MagicMock()

        mock_sessions_resp = MagicMock()
        mock_sessions_resp.json.return_value = [
            {
                "session_key": 9876,
                "meeting_name": "Bahrain Grand Prix",
                "session_name": "Race",
            }
        ]
        mock_sessions_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_meetings_resp, mock_sessions_resp]

        result = resolve_session_key("2024", "Bahrain Grand Prix", "Race")
        self.assertEqual(result, 9876)
        self.assertEqual(mock_get.call_count, 2)

        # First call: meetings endpoint
        args1, kwargs1 = mock_get.call_args_list[0]
        self.assertIn("meetings", args1[0])
        self.assertEqual(kwargs1["params"]["year"], "2024")
        self.assertEqual(kwargs1["params"]["meeting_name"], "Bahrain Grand Prix")

        # Second call: sessions endpoint
        args2, kwargs2 = mock_get.call_args_list[1]
        self.assertIn("sessions", args2[0])
        self.assertEqual(kwargs2["params"]["meeting_key"], 1229)
        self.assertEqual(kwargs2["params"]["session_type"], "Race")

    @patch("main.requests.get")
    def test_no_meeting_match(self, mock_get):
        """No meeting found — should raise ValueError."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(ValueError) as ctx:
            resolve_session_key("2024", "NonExistent", "Race")
        self.assertIn("No meeting found", str(ctx.exception))

    @patch("main.requests.get")
    def test_ambiguous_meetings(self, mock_get):
        """Multiple meetings matched — should raise ValueError."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"meeting_key": 1, "meeting_name": "Bahrain GP"},
            {"meeting_key": 2, "meeting_name": "Bahrain Test"},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(ValueError) as ctx:
            resolve_session_key("2024", "Bahrain", "Race")
        self.assertIn("Ambiguous", str(ctx.exception))
        self.assertIn("2 meetings", str(ctx.exception))

    @patch("main.requests.get")
    def test_no_session_match(self, mock_get):
        """Meeting found but no session — should raise ValueError."""
        mock_meetings_resp = MagicMock()
        mock_meetings_resp.json.return_value = [
            {"meeting_key": 1229, "meeting_name": "Bahrain Grand Prix"}
        ]
        mock_meetings_resp.raise_for_status = MagicMock()

        mock_sessions_resp = MagicMock()
        mock_sessions_resp.json.return_value = []
        mock_sessions_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_meetings_resp, mock_sessions_resp]

        with self.assertRaises(ValueError) as ctx:
            resolve_session_key("2024", "Bahrain Grand Prix", "Race")
        self.assertIn("No session found", str(ctx.exception))

    @patch("main.requests.get")
    def test_ambiguous_sessions(self, mock_get):
        """Meeting found but multiple sessions — should raise ValueError."""
        mock_meetings_resp = MagicMock()
        mock_meetings_resp.json.return_value = [
            {"meeting_key": 1229, "meeting_name": "Bahrain Grand Prix"}
        ]
        mock_meetings_resp.raise_for_status = MagicMock()

        mock_sessions_resp = MagicMock()
        mock_sessions_resp.json.return_value = [
            {"session_key": 1, "meeting_name": "Bahrain GP", "session_name": "Race"},
            {"session_key": 2, "meeting_name": "Bahrain GP", "session_name": "Sprint"},
        ]
        mock_sessions_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_meetings_resp, mock_sessions_resp]

        with self.assertRaises(ValueError) as ctx:
            resolve_session_key("2024", "Bahrain Grand Prix", "Race")
        self.assertIn("Ambiguous", str(ctx.exception))
        self.assertIn("2 sessions", str(ctx.exception))


class TestEndpointConfig(unittest.TestCase):
    """Tests for ENDPOINTS configuration."""

    def test_endpoint_count(self):
        """ENDPOINTS should have exactly 13 entries."""
        self.assertEqual(len(ENDPOINTS), 13)

    def test_all_endpoint_names_present(self):
        """All expected endpoint names should be in ENDPOINTS."""
        expected = {
            "car_data", "laps", "drivers", "intervals", "location",
            "meetings", "pit", "position", "race_control", "sessions",
            "stints", "team_radio", "weather",
        }
        actual = {ep[0] for ep in ENDPOINTS}
        self.assertEqual(actual, expected)


class TestFetchEndpoint(unittest.TestCase):
    """Tests for fetch_endpoint driver filter logic."""

    @patch("main.requests.get")
    def test_driver_filter_applied_when_supported(self, mock_get):
        """driver_number should be in params when supports_driver_filter=True."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"driver_number": 44}]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        fetch_endpoint("car_data", 9876, driver_number="44", supports_driver_filter=True)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"]["driver_number"], "44")

    @patch("main.requests.get")
    def test_driver_filter_not_applied_when_unsupported(self, mock_get):
        """driver_number should NOT be in params when supports_driver_filter=False."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"some": "data"}]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        fetch_endpoint("meetings", 9876, driver_number="44", supports_driver_filter=False)
        _, kwargs = mock_get.call_args
        self.assertNotIn("driver_number", kwargs["params"])

    @patch("main.requests.get")
    def test_driver_filter_not_applied_when_none(self, mock_get):
        """driver_number=None should not add param even when supported."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        fetch_endpoint("car_data", 9876, driver_number=None, supports_driver_filter=True)
        _, kwargs = mock_get.call_args
        self.assertNotIn("driver_number", kwargs["params"])


class TestProduceRecords(unittest.TestCase):
    """Tests for produce_records batch logic."""

    def test_correct_produce_calls(self):
        """Should call produce() once per record and flush() once."""
        mock_producer = MagicMock()
        topic = MagicMock()
        records = [
            {"driver_number": 44, "speed": 300},
            {"driver_number": 44, "speed": 310},
            {"driver_number": 1, "speed": 295},
        ]

        produced = produce_records(mock_producer, topic, records, "driver_number", batch_size=2)

        self.assertEqual(produced, 3)
        self.assertEqual(mock_producer.produce.call_count, 3)
        mock_producer.flush.assert_called_once()

    def test_produce_with_no_key_field(self):
        """When key_field is None, key should be None."""
        mock_producer = MagicMock()
        topic = MagicMock()
        records = [{"flag": "green"}, {"flag": "yellow"}]

        produced = produce_records(mock_producer, topic, records, None, batch_size=500)

        self.assertEqual(produced, 2)
        for c in mock_producer.produce.call_args_list:
            self.assertIsNone(c.kwargs.get("key") or c[1].get("key"))

    def test_batch_chunking(self):
        """Records should be produced in batches of batch_size."""
        mock_producer = MagicMock()
        topic = MagicMock()
        records = [{"driver_number": i} for i in range(5)]

        produced = produce_records(mock_producer, topic, records, "driver_number", batch_size=2)

        self.assertEqual(produced, 5)
        self.assertEqual(mock_producer.produce.call_count, 5)
        # Values should be bytes-encoded JSON
        first_call = mock_producer.produce.call_args_list[0]
        value = first_call.kwargs.get("value") or first_call[1].get("value")
        self.assertIsInstance(value, bytes)


if __name__ == "__main__":
    unittest.main()
