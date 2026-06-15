import unittest
from unittest.mock import patch, MagicMock, call
import json

from main import (
    resolve_session_key,
    fetch_endpoint,
    fetch_endpoint_with_retry,
    fetch_drivers_for_session,
    produce_records,
    ENDPOINTS,
    PER_DRIVER_ENDPOINTS,
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

    def test_per_driver_endpoints_are_subset(self):
        """PER_DRIVER_ENDPOINTS should be a subset of all endpoint names."""
        all_names = {ep[0] for ep in ENDPOINTS}
        self.assertTrue(PER_DRIVER_ENDPOINTS.issubset(all_names))

    def test_per_driver_endpoints_support_driver_filter(self):
        """All PER_DRIVER_ENDPOINTS must have supports_driver_filter=True."""
        for endpoint, _, supports_driver, _ in ENDPOINTS:
            if endpoint in PER_DRIVER_ENDPOINTS:
                self.assertTrue(
                    supports_driver,
                    f"{endpoint} is in PER_DRIVER_ENDPOINTS but supports_driver_filter is False",
                )


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


class TestFetchDriversForSession(unittest.TestCase):
    """Tests for fetch_drivers_for_session."""

    @patch("main.requests.get")
    def test_returns_sorted_unique_driver_numbers(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"driver_number": 44, "name": "Hamilton"},
            {"driver_number": 1, "name": "Verstappen"},
            {"driver_number": 44, "name": "Hamilton"},  # duplicate
            {"driver_number": 16, "name": "Leclerc"},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = fetch_drivers_for_session(9876)
        self.assertEqual(result, [1, 16, 44])

    @patch("main.requests.get")
    def test_empty_drivers(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = fetch_drivers_for_session(9876)
        self.assertEqual(result, [])

    @patch("main.requests.get")
    def test_skips_entries_without_driver_number(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"driver_number": 44, "name": "Hamilton"},
            {"name": "Unknown"},  # no driver_number key
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = fetch_drivers_for_session(9876)
        self.assertEqual(result, [44])


class TestFetchEndpointWithRetry(unittest.TestCase):
    """Tests for fetch_endpoint_with_retry retry logic."""

    @patch("main.time.sleep")
    @patch("main.requests.get")
    def test_no_retry_on_422(self, mock_get, mock_sleep):
        """422 should NOT be retried — it's a client error."""
        mock_resp = MagicMock()
        mock_resp.status_code = 422
        mock_resp.raise_for_status.side_effect = __import__("requests").exceptions.HTTPError(
            response=mock_resp
        )
        mock_get.return_value = mock_resp

        with self.assertRaises(__import__("requests").exceptions.HTTPError):
            fetch_endpoint_with_retry("car_data", 9876, max_retries=3)

        # Should only try once (no retry)
        self.assertEqual(mock_get.call_count, 1)
        mock_sleep.assert_not_called()

    @patch("main.time.sleep")
    @patch("main.requests.get")
    def test_no_retry_on_400(self, mock_get, mock_sleep):
        """400 should NOT be retried."""
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.raise_for_status.side_effect = __import__("requests").exceptions.HTTPError(
            response=mock_resp
        )
        mock_get.return_value = mock_resp

        with self.assertRaises(__import__("requests").exceptions.HTTPError):
            fetch_endpoint_with_retry("car_data", 9876, max_retries=3)

        self.assertEqual(mock_get.call_count, 1)

    @patch("main.time.sleep")
    @patch("main.requests.get")
    def test_retry_on_429(self, mock_get, mock_sleep):
        """429 should be retried with backoff."""
        mock_resp_429 = MagicMock()
        mock_resp_429.status_code = 429
        mock_resp_429.raise_for_status.side_effect = __import__("requests").exceptions.HTTPError(
            response=mock_resp_429
        )

        mock_resp_ok = MagicMock()
        mock_resp_ok.json.return_value = [{"data": 1}]
        mock_resp_ok.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp_429, mock_resp_ok]

        result = fetch_endpoint_with_retry("car_data", 9876, max_retries=3)
        self.assertEqual(result, [{"data": 1}])
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()

    @patch("main.time.sleep")
    @patch("main.requests.get")
    def test_retry_on_500(self, mock_get, mock_sleep):
        """500 should be retried."""
        mock_resp_500 = MagicMock()
        mock_resp_500.status_code = 500
        mock_resp_500.raise_for_status.side_effect = __import__("requests").exceptions.HTTPError(
            response=mock_resp_500
        )

        mock_resp_ok = MagicMock()
        mock_resp_ok.json.return_value = [{"data": 1}]
        mock_resp_ok.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp_500, mock_resp_ok]

        result = fetch_endpoint_with_retry("car_data", 9876, max_retries=3)
        self.assertEqual(result, [{"data": 1}])
        self.assertEqual(mock_get.call_count, 2)


class TestProduceRecords(unittest.TestCase):
    """Tests for produce_records using topic.serialize() pattern."""

    def test_correct_produce_calls(self):
        """Should call topic.serialize() + produce() per record, flush() once."""
        mock_producer = MagicMock()
        topic = MagicMock()
        topic.serialize.return_value = MagicMock(key=b"44", value=b'{"speed":300}')
        records = [
            {"driver_number": 44, "speed": 300},
            {"driver_number": 44, "speed": 310},
            {"driver_number": 1, "speed": 295},
        ]

        produced = produce_records(mock_producer, topic, records, "driver_number", batch_size=2)

        self.assertEqual(produced, 3)
        self.assertEqual(topic.serialize.call_count, 3)
        self.assertEqual(mock_producer.produce.call_count, 3)
        mock_producer.flush.assert_called_once()

    def test_serialize_called_with_raw_dict(self):
        """topic.serialize should receive the raw dict, not json.dumps."""
        mock_producer = MagicMock()
        topic = MagicMock()
        topic.serialize.return_value = MagicMock(key=b"44", value=b'{"speed":300}')
        record = {"driver_number": 44, "speed": 300}

        produce_records(mock_producer, topic, [record], "driver_number", batch_size=500)

        topic.serialize.assert_called_once_with(key="44", value=record)

    def test_produce_uses_serialized_msg(self):
        """producer.produce should use msg.key and msg.value from serialize."""
        mock_producer = MagicMock()
        topic = MagicMock()
        mock_msg = MagicMock(key=b"serialized-key", value=b"serialized-value")
        topic.serialize.return_value = mock_msg

        produce_records(mock_producer, topic, [{"driver_number": 1}], "driver_number", batch_size=500)

        mock_producer.produce.assert_called_once_with(
            topic=topic.name, key=b"serialized-key", value=b"serialized-value"
        )

    def test_produce_with_no_key_field(self):
        """When key_field is None, key should be None in serialize call."""
        mock_producer = MagicMock()
        topic = MagicMock()
        topic.serialize.return_value = MagicMock(key=None, value=b'{"flag":"green"}')
        records = [{"flag": "green"}, {"flag": "yellow"}]

        produced = produce_records(mock_producer, topic, records, None, batch_size=500)

        self.assertEqual(produced, 2)
        for c in topic.serialize.call_args_list:
            self.assertIsNone(c.kwargs["key"])

    def test_batch_chunking(self):
        """Records should be produced in batches of batch_size."""
        mock_producer = MagicMock()
        topic = MagicMock()
        topic.serialize.return_value = MagicMock(key=b"0", value=b"{}")
        records = [{"driver_number": i} for i in range(5)]

        produced = produce_records(mock_producer, topic, records, "driver_number", batch_size=2)

        self.assertEqual(produced, 5)
        self.assertEqual(mock_producer.produce.call_count, 5)


if __name__ == "__main__":
    unittest.main()
