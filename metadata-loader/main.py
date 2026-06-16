"""
Metadata Loader — reads openf1-drivers, openf1-meetings, openf1-sessions
and writes each record as a versioned config entry to the Dynamic Configuration
Manager REST API.

Config keys:
  - driver/<driver_number>  → full driver record
  - meeting/<meeting_key>   → full meeting record
  - session/<session_key>   → full session record
"""
import os
import json
import logging
import time
import threading
import requests
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONFIG_SERVICE_URL = os.environ.get("CONFIG_SERVICE_URL", "http://config-manager:80")

# --- Config service helpers -----------------------------------------------

def _put_config(key: str, value: dict, max_retries: int = 5) -> None:
    """PUT a config entry, retrying on transient errors."""
    url = f"{CONFIG_SERVICE_URL}/config/{key}"
    for attempt in range(max_retries):
        try:
            resp = requests.put(url, json=value, timeout=10)
            if resp.status_code in (200, 201, 204):
                logger.debug("Config set: %s", key)
                return
            logger.warning("Config PUT %s → HTTP %d: %s", key, resp.status_code, resp.text[:200])
        except requests.RequestException as exc:
            logger.warning("Config PUT %s attempt %d failed: %s", key, attempt + 1, exc)
        time.sleep(2 ** attempt)
    logger.error("Failed to set config key %s after %d attempts", key, max_retries)


def _wait_for_config_service(timeout: int = 120) -> None:
    """Block until the config service health endpoint responds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{CONFIG_SERVICE_URL}/health", timeout=5)
            if resp.status_code < 500:
                logger.info("Config service ready.")
                return
        except requests.RequestException:
            pass
        logger.info("Waiting for config service at %s ...", CONFIG_SERVICE_URL)
        time.sleep(5)
    raise RuntimeError(f"Config service not ready after {timeout}s")


# --- Message handlers -------------------------------------------------------

def handle_driver(row: dict) -> dict:
    driver_number = row.get("driver_number")
    if driver_number is None:
        logger.warning("Driver record missing driver_number: %s", row)
        return row
    key = f"driver/{driver_number}"
    _put_config(key, row)
    logger.info("Loaded driver config: %s → %s", key, row.get("full_name", "?"))
    return row


def handle_meeting(row: dict) -> dict:
    meeting_key = row.get("meeting_key")
    if meeting_key is None:
        logger.warning("Meeting record missing meeting_key: %s", row)
        return row
    key = f"meeting/{meeting_key}"
    _put_config(key, row)
    logger.info("Loaded meeting config: %s → %s", key, row.get("meeting_name", "?"))
    return row


def handle_session(row: dict) -> dict:
    session_key = row.get("session_key")
    if session_key is None:
        logger.warning("Session record missing session_key: %s", row)
        return row
    key = f"session/{session_key}"
    _put_config(key, row)
    logger.info("Loaded session config: %s → %s", key, row.get("session_name", "?"))
    return row


# --- Application ------------------------------------------------------------

def main():
    _wait_for_config_service()

    app = Application(consumer_group="metadata-loader-v1", auto_offset_reset="earliest")

    drivers_topic = app.topic(os.environ["input_drivers"], value_deserializer="json")
    meetings_topic = app.topic(os.environ["input_meetings"], value_deserializer="json")
    sessions_topic = app.topic(os.environ["input_sessions"], value_deserializer="json")

    sdf_drivers = app.dataframe(drivers_topic).update(handle_driver)
    sdf_meetings = app.dataframe(meetings_topic).update(handle_meeting)
    sdf_sessions = app.dataframe(sessions_topic).update(handle_session)

    app.run()


if __name__ == "__main__":
    main()
