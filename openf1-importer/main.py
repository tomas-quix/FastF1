import os
import time
import logging
from dotenv import load_dotenv

load_dotenv()

import requests
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("OPENF1_BASE_URL", "https://api.openf1.org/v1").rstrip("/")
YEAR = os.environ.get("OPENF1_YEAR", "2023")
MEETING = os.environ.get("OPENF1_MEETING", "Monza")
SESSION_NAME = os.environ.get("OPENF1_SESSION_NAME", "Race")
SESSION_KEY_OVERRIDE = os.environ.get("OPENF1_SESSION_KEY", "").strip()
INCLUDE_CAR_DATA = os.environ.get("INCLUDE_CAR_DATA", "true").lower() == "true"

TOPIC_VARS = {
    "sessions":     "output_sessions",
    "drivers":      "output_drivers",
    "laps":         "output_laps",
    "stints":       "output_stints",
    "pit":          "output_pit",
    "position":     "output_position",
    "intervals":    "output_intervals",
    "weather":      "output_weather",
    "race_control": "output_race_control",
    "team_radio":   "output_team_radio",
    "car_data":     "output_car_data",
}


def get_topic_name(endpoint_key: str) -> str:
    env_var = TOPIC_VARS[endpoint_key]
    return os.environ.get(env_var, f"openf1-{endpoint_key.replace('_', '-')}")


def fetch_with_retry(url: str, params: dict = None, max_retries: int = 5) -> list:
    """Fetch a URL with exponential backoff on 429/5xx."""
    delay = 2
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 429:
                wait = delay * (2 ** attempt)
                logger.warning(f"Rate limited. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logger.warning(f"Request failed ({e}). Retry {attempt+1}/{max_retries} in {wait}s...")
            time.sleep(wait)
    return []


def resolve_session_key() -> int:
    """Resolve the session_key from year/meeting/session_name, or use override."""
    if SESSION_KEY_OVERRIDE:
        logger.info(f"Using OPENF1_SESSION_KEY override: {SESSION_KEY_OVERRIDE}")
        return int(SESSION_KEY_OVERRIDE)

    logger.info(f"Resolving session: year={YEAR}, meeting={MEETING}, session={SESSION_NAME}")
    sessions = fetch_with_retry(f"{BASE_URL}/sessions", {
        "year": YEAR,
        "session_name": SESSION_NAME,
    })

    if not sessions:
        raise ValueError(f"No sessions found for year={YEAR}, session_name={SESSION_NAME}")

    meeting_lower = MEETING.lower()
    matches = [
        s for s in sessions
        if meeting_lower in (s.get("circuit_short_name") or "").lower()
        or meeting_lower in (s.get("meeting_name") or "").lower()
        or meeting_lower in (s.get("location") or "").lower()
        or meeting_lower in (s.get("country_name") or "").lower()
    ]

    if len(matches) == 0:
        available = [(s.get("meeting_name"), s.get("location"), s.get("circuit_short_name")) for s in sessions]
        raise ValueError(f"No session matched meeting='{MEETING}'. Available: {available}")
    if len(matches) > 1:
        available = [(s.get("meeting_name"), s.get("session_key")) for s in matches]
        raise ValueError(
            f"Multiple sessions matched meeting='{MEETING}'. Use OPENF1_SESSION_KEY to disambiguate. Matches: {available}"
        )

    session = matches[0]
    key = session["session_key"]
    logger.info(f"Resolved session_key={key} — {session.get('meeting_name')} @ {session.get('location')} ({session.get('date_start', '')})")
    return key


def main():
    app = Application()

    # Build topic objects keyed by endpoint
    topics = {
        key: app.topic(get_topic_name(key), value_serializer="json")
        for key in TOPIC_VARS
    }

    session_key = resolve_session_key()
    counts = {}

    def publish_endpoint(endpoint: str, params: dict, key_field: str = "driver_number"):
        url = f"{BASE_URL}/{endpoint}"
        logger.info(f"Fetching {endpoint} ...")
        records = fetch_with_retry(url, params)
        topic = topics[endpoint]
        n = 0
        with app.get_producer() as producer:
            for record in records:
                msg_key = str(record.get(key_field, session_key))
                msg = topic.serialize(key=msg_key, value=record)
                producer.produce(topic=topic, key=msg.key, value=msg.value)
                n += 1
        counts[endpoint] = n
        logger.info(f"  → {n} records published to {get_topic_name(endpoint)}")

    base_params = {"session_key": session_key}

    # --- Non-car-data endpoints ---
    publish_endpoint("sessions",     {"session_key": session_key}, key_field="session_key")
    publish_endpoint("drivers",      base_params, key_field="driver_number")
    publish_endpoint("laps",         base_params, key_field="driver_number")
    publish_endpoint("stints",       base_params, key_field="driver_number")
    publish_endpoint("pit",          base_params, key_field="driver_number")
    publish_endpoint("position",     base_params, key_field="driver_number")
    publish_endpoint("intervals",    base_params, key_field="driver_number")
    publish_endpoint("weather",      base_params, key_field="meeting_key")
    publish_endpoint("race_control", base_params, key_field="meeting_key")
    publish_endpoint("team_radio",   base_params, key_field="driver_number")

    # --- Car data (high-frequency — per driver to avoid oversized responses) ---
    if INCLUDE_CAR_DATA:
        logger.info("Fetching car_data per driver ...")
        drivers_data = fetch_with_retry(f"{BASE_URL}/drivers", base_params)
        driver_numbers = [str(d["driver_number"]) for d in drivers_data if "driver_number" in d]
        logger.info(f"  Drivers found: {driver_numbers}")
        car_topic = topics["car_data"]
        car_count = 0
        with app.get_producer() as producer:
            for dn in driver_numbers:
                logger.info(f"  Fetching car_data for driver {dn} ...")
                records = fetch_with_retry(
                    f"{BASE_URL}/car_data",
                    {"session_key": session_key, "driver_number": dn},
                )
                for record in records:
                    msg = car_topic.serialize(key=dn, value=record)
                    producer.produce(topic=car_topic, key=msg.key, value=msg.value)
                    car_count += 1
                logger.info(f"    → {len(records)} records for driver {dn}")
                time.sleep(0.2)  # polite pacing between per-driver requests
        counts["car_data"] = car_count
        logger.info(f"  → {car_count} total car_data records published to {get_topic_name('car_data')}")
    else:
        logger.info("INCLUDE_CAR_DATA=false — skipping car telemetry.")
        counts["car_data"] = 0

    # Summary
    logger.info("=== Import complete ===")
    total = sum(counts.values())
    for ep, n in counts.items():
        logger.info(f"  {ep}: {n} records")
    logger.info(f"  TOTAL: {total} records")


if __name__ == "__main__":
    main()
