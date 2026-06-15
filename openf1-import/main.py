import os
import sys
import time
import logging
import requests
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openf1.org/v1"

# 13 endpoint configurations
# Each tuple: (endpoint_path, env_var_for_topic, supports_driver_filter, message_key_field)
ENDPOINTS = [
    ("car_data",      "output_car_data",      True,  "driver_number"),
    ("laps",          "output_laps",           True,  "driver_number"),
    ("drivers",       "output_drivers",        False, "driver_number"),
    ("intervals",     "output_intervals",      True,  "driver_number"),
    ("location",      "output_location",       True,  "driver_number"),
    ("meetings",      "output_meetings",       False, None),
    ("pit",           "output_pit",            True,  "driver_number"),
    ("position",      "output_position",       True,  "driver_number"),
    ("race_control",  "output_race_control",   False, None),
    ("sessions",      "output_sessions",       False, None),
    ("stints",        "output_stints",         True,  "driver_number"),
    ("team_radio",    "output_team_radio",     True,  "driver_number"),
    ("weather",       "output_weather",        False, None),
]

# Endpoints that MUST be fetched per-driver (bulk requests return 422)
PER_DRIVER_ENDPOINTS = {"car_data", "location"}


def resolve_session_key(year, meeting_name, session_type):
    """Resolve OpenF1 session_key from year, meeting name, and session type.

    Uses a two-step lookup: first resolves the meeting_key from the meetings
    endpoint, then queries sessions filtered by meeting_key and session_type.
    The sessions endpoint does not support meeting_name as a filter directly.
    """
    # Step 1: resolve meeting_key from meeting_name
    meetings_url = f"{BASE_URL}/meetings"
    meetings_params = {"year": year, "meeting_name": meeting_name}
    resp = requests.get(meetings_url, params=meetings_params, timeout=30)
    resp.raise_for_status()
    meetings = resp.json()

    if len(meetings) == 0:
        raise ValueError(
            f"No meeting found for year={year}, meeting_name='{meeting_name}'. "
            f"Check your parameters."
        )
    if len(meetings) > 1:
        names = [m.get("meeting_name", "?") for m in meetings]
        raise ValueError(
            f"Ambiguous: {len(meetings)} meetings matched for "
            f"meeting_name='{meeting_name}'. Matches: {names}"
        )

    meeting_key = meetings[0]["meeting_key"]
    logger.info("Resolved meeting: %s (meeting_key=%s)", meetings[0].get("meeting_name"), meeting_key)

    # Step 2: resolve session_key from meeting_key + session_type
    sessions_url = f"{BASE_URL}/sessions"
    sessions_params = {"meeting_key": meeting_key, "session_type": session_type}
    resp = requests.get(sessions_url, params=sessions_params, timeout=30)
    resp.raise_for_status()
    sessions = resp.json()

    if len(sessions) == 0:
        raise ValueError(
            f"No session found for meeting_key={meeting_key}, "
            f"session_type='{session_type}'. Check your parameters."
        )
    if len(sessions) > 1:
        names = [f"{s.get('meeting_name', '?')} - {s.get('session_name', '?')}" for s in sessions]
        raise ValueError(
            f"Ambiguous: {len(sessions)} sessions matched. Narrow your search. "
            f"Matches: {names}"
        )

    session = sessions[0]
    session_key = session["session_key"]
    logger.info(
        "Resolved session: %s - %s (session_key=%s)",
        session.get("meeting_name"), session.get("session_name"), session_key,
    )
    return session_key


def fetch_drivers_for_session(session_key):
    """Fetch all driver numbers for a given session."""
    url = f"{BASE_URL}/drivers"
    params = {"session_key": session_key}
    logger.info("Fetching driver list for session %s ...", session_key)
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    drivers = resp.json()
    driver_numbers = sorted({d["driver_number"] for d in drivers if "driver_number" in d})
    logger.info("  -> Found %d drivers: %s", len(driver_numbers), driver_numbers)
    return driver_numbers


def fetch_endpoint(endpoint, session_key, driver_number=None, supports_driver_filter=False):
    """Fetch all records from an OpenF1 endpoint for the given session."""
    url = f"{BASE_URL}/{endpoint}"
    params = {"session_key": session_key}
    if driver_number and supports_driver_filter:
        params["driver_number"] = driver_number

    logger.info("Fetching %s with params %s ...", endpoint, params)
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    records = resp.json()
    logger.info("  -> %d records fetched from %s", len(records), endpoint)
    return records


def fetch_endpoint_with_retry(endpoint, session_key, driver_number=None,
                               supports_driver_filter=False, max_retries=3):
    """Fetch with exponential backoff retry for 429/5xx errors."""
    for attempt in range(max_retries):
        try:
            return fetch_endpoint(endpoint, session_key, driver_number, supports_driver_filter)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status not in (400, 401, 403, 404, 422) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                logger.warning("HTTP %d on %s, retrying in %ds...", status, endpoint, wait_time)
                time.sleep(wait_time)
            else:
                raise
    return []


def produce_records(producer, topic, records, key_field, batch_size):
    """Produce records to a Kafka topic in batches."""
    produced = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        for record in batch:
            key = str(record.get(key_field, "")) if key_field else None
            msg = topic.serialize(key=key, value=record)
            producer.produce(topic=topic, key=msg.key, value=msg.value)
        produced += len(batch)
    # Flush after all batches for this endpoint
    producer.flush()
    return produced


def main():
    year = os.environ["YEAR"]
    meeting_name = os.environ["MEETING_NAME"]
    session_type = os.environ.get("SESSION_TYPE", "Race")
    driver_number = os.environ.get("DRIVER_NUMBER", "").strip() or None
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))

    logger.info("=== OpenF1 Import Job ===")
    logger.info("Year: %s | Meeting: %s | Session: %s | Driver: %s | Batch: %d",
                year, meeting_name, session_type, driver_number or "ALL", batch_size)

    # Step 1: Resolve session
    session_key = resolve_session_key(year, meeting_name, session_type)

    # Step 2: Set up Quix Streams
    app = Application(consumer_group="openf1-import")

    # Create topic objects for each endpoint
    topics = {}
    for endpoint, env_var, _, _ in ENDPOINTS:
        topic_name = os.environ.get(env_var)
        if topic_name:
            topics[endpoint] = app.topic(topic_name, value_serializer="json")

    # Step 3: Fetch driver list for per-driver endpoints
    all_driver_numbers = []
    if not driver_number:
        try:
            all_driver_numbers = fetch_drivers_for_session(session_key)
        except Exception as e:
            logger.error("Failed to fetch drivers list: %s", e)
            logger.warning("Per-driver endpoints (car_data, location) will be skipped.")

    # Step 4: Fetch and produce for each endpoint
    total_records = 0
    start_time = time.time()

    with app.get_producer() as producer:
        for endpoint, env_var, supports_driver, key_field in ENDPOINTS:
            if endpoint not in topics:
                logger.warning("Skipping %s — no topic configured", endpoint)
                continue

            ep_start = time.time()

            # Per-driver endpoints need individual driver fetches when no specific driver set
            if endpoint in PER_DRIVER_ENDPOINTS and not driver_number:
                if not all_driver_numbers:
                    logger.warning("Skipping %s — no drivers available", endpoint)
                    continue

                ep_records = 0
                for dn in all_driver_numbers:
                    try:
                        records = fetch_endpoint_with_retry(
                            endpoint, session_key, str(dn), supports_driver
                        )
                    except Exception as e:
                        logger.error("Failed to fetch %s for driver %s: %s", endpoint, dn, e)
                        continue

                    if records:
                        produced = produce_records(producer, topics[endpoint], records, key_field, batch_size)
                        ep_records += produced
                        logger.info("  -> Driver %s: %d records", dn, produced)

                    time.sleep(0.2)

                elapsed = time.time() - ep_start
                total_records += ep_records
                logger.info("  -> Produced %d total records to %s in %.1fs", ep_records, os.environ.get(env_var), elapsed)
            else:
                try:
                    records = fetch_endpoint_with_retry(
                        endpoint, session_key, driver_number, supports_driver
                    )
                except Exception as e:
                    logger.error("Failed to fetch %s: %s", endpoint, e)
                    continue

                if not records:
                    logger.info("  -> No records for %s, skipping.", endpoint)
                    continue

                produced = produce_records(producer, topics[endpoint], records, key_field, batch_size)
                elapsed = time.time() - ep_start
                total_records += produced
                logger.info("  -> Produced %d records to %s in %.1fs", produced, os.environ.get(env_var), elapsed)

            # Be polite to the API
            time.sleep(0.5)

    elapsed_total = time.time() - start_time
    logger.info("=== Import complete: %d total records across %d endpoints in %.1fs ===",
                total_records, len(ENDPOINTS), elapsed_total)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Job failed: %s", e, exc_info=True)
        sys.exit(1)
