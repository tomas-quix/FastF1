"""Unit tests for metadata-loader mapping logic."""
import pytest
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

# Patch out the config service wait and requests before importing
with patch.dict(os.environ, {
    "CONFIG_SERVICE_URL": "http://localhost:9999",
    "input_drivers": "openf1-drivers",
    "input_meetings": "openf1-meetings",
    "input_sessions": "openf1-sessions",
}):
    import main as loader


# --- _put_config tests -------------------------------------------------------

def test_put_config_success():
    with patch("main.requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        loader._put_config("driver/1", {"name": "Max"})
        mock_put.assert_called_once()
        call_args = mock_put.call_args
        assert "/config/driver/1" in call_args[0][0]
        assert call_args[1]["json"] == {"name": "Max"}


def test_put_config_retries_on_5xx():
    with patch("main.requests.put") as mock_put, patch("main.time.sleep"):
        mock_put.return_value.status_code = 503
        loader._put_config("driver/1", {}, max_retries=3)
        assert mock_put.call_count == 3


def test_put_config_success_after_retry():
    responses = [MagicMock(status_code=503), MagicMock(status_code=200)]
    with patch("main.requests.put", side_effect=responses), patch("main.time.sleep"):
        loader._put_config("driver/1", {"name": "Max"}, max_retries=3)


# --- handle_driver tests -----------------------------------------------------

def test_handle_driver_normal():
    row = {"driver_number": 1, "full_name": "Max Verstappen", "team_name": "Red Bull Racing"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_driver(row)
        mock_put.assert_called_once_with("driver/1", row)
        assert result == row


def test_handle_driver_missing_number():
    row = {"full_name": "Unknown"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_driver(row)
        mock_put.assert_not_called()
        assert result == row


def test_handle_driver_zero_number():
    """driver_number=0 is falsy — treat as valid (not None)."""
    row = {"driver_number": 0, "full_name": "Safety Car"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_driver(row)
        mock_put.assert_called_once_with("driver/0", row)


# --- handle_meeting tests ----------------------------------------------------

def test_handle_meeting_normal():
    row = {"meeting_key": 1229, "meeting_name": "Bahrain Grand Prix", "circuit_key": 3}
    with patch("main._put_config") as mock_put:
        result = loader.handle_meeting(row)
        mock_put.assert_called_once_with("meeting/1229", row)
        assert result == row


def test_handle_meeting_missing_key():
    row = {"meeting_name": "Unknown"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_meeting(row)
        mock_put.assert_not_called()


# --- handle_session tests ----------------------------------------------------

def test_handle_session_normal():
    row = {"session_key": 9161, "session_name": "Race", "session_type": "Race"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_session(row)
        mock_put.assert_called_once_with("session/9161", row)
        assert result == row


def test_handle_session_missing_key():
    row = {"session_name": "Qualifying"}
    with patch("main._put_config") as mock_put:
        result = loader.handle_session(row)
        mock_put.assert_not_called()
