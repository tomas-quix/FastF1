import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Ensure the parent directory is on the path so we can import from main.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch quixstreams before importing main to avoid needing a real broker
import unittest.mock as mock

_qs_mock = mock.MagicMock()
sys.modules.setdefault("quixstreams", _qs_mock)
sys.modules.setdefault("quixstreams.sources", _qs_mock)

import importlib
import types


def _load_main(env_overrides: dict):
    """Import main.py with specific env vars set, returning the module."""
    with patch.dict(os.environ, env_overrides, clear=False):
        import main as m
        importlib.reload(m)
        return m


class TestResolveSessionKey(unittest.TestCase):

    SESSIONS = [
        {
            "session_key": 9149,
            "meeting_name": "Italian Grand Prix",
            "location": "Monza",
            "circuit_short_name": "Monza",
            "country_name": "Italy",
            "session_name": "Race",
            "year": 2023,
        },
        {
            "session_key": 9100,
            "meeting_name": "British Grand Prix",
            "location": "Silverstone",
            "circuit_short_name": "Silverstone",
            "country_name": "United Kingdom",
            "session_name": "Race",
            "year": 2023,
        },
    ]

    def _mock_get(self, sessions_data):
        """Return a mock requests.get that returns `sessions_data` as JSON."""
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = sessions_data
        resp.raise_for_status.return_value = None
        mock_get = MagicMock(return_value=resp)
        return mock_get

    def test_correct_match_monza(self):
        """resolve_session_key returns 9149 when MEETING=Monza."""
        env = {
            "OPENF1_YEAR": "2023",
            "OPENF1_MEETING": "Monza",
            "OPENF1_SESSION_NAME": "Race",
            "OPENF1_SESSION_KEY": "",
            "OPENF1_BASE_URL": "https://api.openf1.org/v1",
        }
        m = _load_main(env)
        with patch("requests.get", self._mock_get(self.SESSIONS)):
            result = m.resolve_session_key()
        self.assertEqual(result, 9149)

    def test_session_key_override(self):
        """resolve_session_key returns the override value without HTTP calls."""
        env = {
            "OPENF1_SESSION_KEY": "9999",
            "OPENF1_BASE_URL": "https://api.openf1.org/v1",
        }
        m = _load_main(env)
        with patch("requests.get") as mock_get:
            result = m.resolve_session_key()
            mock_get.assert_not_called()
        self.assertEqual(result, 9999)

    def test_no_match_raises_value_error(self):
        """resolve_session_key raises ValueError when no session matches the meeting."""
        no_monza_sessions = [
            {
                "session_key": 9100,
                "meeting_name": "British Grand Prix",
                "location": "Silverstone",
                "circuit_short_name": "Silverstone",
                "country_name": "United Kingdom",
            },
        ]
        env = {
            "OPENF1_YEAR": "2023",
            "OPENF1_MEETING": "Monza",
            "OPENF1_SESSION_NAME": "Race",
            "OPENF1_SESSION_KEY": "",
            "OPENF1_BASE_URL": "https://api.openf1.org/v1",
        }
        m = _load_main(env)
        with patch("requests.get", self._mock_get(no_monza_sessions)):
            with self.assertRaises(ValueError):
                m.resolve_session_key()

    def test_multiple_matches_raises_value_error(self):
        """resolve_session_key raises ValueError when multiple sessions match."""
        two_monza = [
            {
                "session_key": 9149,
                "meeting_name": "Italian Grand Prix",
                "location": "Monza",
                "circuit_short_name": "Monza",
                "country_name": "Italy",
            },
            {
                "session_key": 9150,
                "meeting_name": "Monza Test",
                "location": "Monza",
                "circuit_short_name": "Monza",
                "country_name": "Italy",
            },
        ]
        env = {
            "OPENF1_YEAR": "2023",
            "OPENF1_MEETING": "Monza",
            "OPENF1_SESSION_NAME": "Race",
            "OPENF1_SESSION_KEY": "",
            "OPENF1_BASE_URL": "https://api.openf1.org/v1",
        }
        m = _load_main(env)
        with patch("requests.get", self._mock_get(two_monza)):
            with self.assertRaises(ValueError):
                m.resolve_session_key()


class TestFetchWithRetry(unittest.TestCase):

    def _make_response(self, status_code, json_data=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or []
        if status_code >= 400:
            resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_429_backoff_then_success(self):
        """fetch_with_retry retries after 429 and returns data on success."""
        env = {"OPENF1_BASE_URL": "https://api.openf1.org/v1"}
        m = _load_main(env)

        r429 = self._make_response(429)
        r200 = self._make_response(200, [{"foo": "bar"}])
        mock_get = MagicMock(side_effect=[r429, r429, r200])

        with patch("requests.get", mock_get), patch("time.sleep") as mock_sleep:
            result = m.fetch_with_retry("https://api.openf1.org/v1/sessions")

        self.assertEqual(result, [{"foo": "bar"}])
        self.assertEqual(mock_sleep.call_count, 2)

    def test_success_on_first_try(self):
        """fetch_with_retry returns data immediately on 200 without sleeping."""
        env = {"OPENF1_BASE_URL": "https://api.openf1.org/v1"}
        m = _load_main(env)

        r200 = self._make_response(200, [{"session_key": 9149}])
        mock_get = MagicMock(return_value=r200)

        with patch("requests.get", mock_get), patch("time.sleep") as mock_sleep:
            result = m.fetch_with_retry("https://api.openf1.org/v1/sessions")

        self.assertEqual(result, [{"session_key": 9149}])
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
