import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.openf1.org/v1"
MAX_RETRIES = 5
REQUEST_TIMEOUT = 30


def _request_with_retry(session, url, params=None, max_retries=MAX_RETRIES, timeout=REQUEST_TIMEOUT):
    """GET a URL with exponential backoff retry, honoring HTTP 429 / Retry-After."""
    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(url, params=params, timeout=timeout)
        except requests.exceptions.RequestException as e:
            if attempt >= max_retries:
                raise
            wait_time = 2 ** attempt
            logger.warning("Request error on %s (attempt %d/%d): %s. Retrying in %ds...",
                           url, attempt, max_retries, e, wait_time)
            time.sleep(wait_time)
            continue

        if response.status_code == 429 or response.status_code >= 500:
            if attempt >= max_retries:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2 ** attempt
            else:
                wait_time = 2 ** attempt
            logger.warning("HTTP %d on %s (attempt %d/%d). Retrying in %.1fs...",
                           response.status_code, url, attempt, max_retries, wait_time)
            time.sleep(wait_time)
            continue

        response.raise_for_status()
        return response


def resolve_session_key(session, base_url, year, track, session_name):
    """Resolve the OpenF1 session_key for the given year, track and session name.

    Queries the /sessions endpoint filtered by year and session_name, then
    matches the track argument case-insensitively against either
    circuit_short_name or country_name.
    """
    url = f"{base_url}/sessions"
    params = {"year": year, "session_name": session_name}
    logger.info("Looking up sessions with params %s", params)
    response = _request_with_retry(session, url, params=params)
    sessions = response.json()

    track_lower = track.strip().lower()

    def matches_track(s):
        circuit = str(s.get("circuit_short_name", "")).strip().lower()
        country = str(s.get("country_name", "")).strip().lower()
        return track_lower == circuit or track_lower == country

    candidates = [s for s in sessions if matches_track(s)]

    if not candidates:
        logger.error(
            "No sessions found for year=%s, track='%s', session_name='%s'. "
            "Searched %d session(s) returned by the API.",
            year, track, session_name, len(sessions),
        )
        raise ValueError(
            f"No session found matching year={year}, track='{track}', "
            f"session_name='{session_name}'."
        )

    if len(candidates) > 1:
        logger.warning(
            "Multiple sessions (%d) matched year=%s, track='%s', session_name='%s':",
            len(candidates), year, track, session_name,
        )
        for c in candidates:
            logger.warning(
                "  session_key=%s circuit_short_name=%s country_name=%s session_name=%s date=%s",
                c.get("session_key"), c.get("circuit_short_name"),
                c.get("country_name"), c.get("session_name"), c.get("date_start"),
            )

        exact_matches = [
            c for c in candidates
            if str(c.get("circuit_short_name", "")).strip().lower() == track_lower
            or str(c.get("country_name", "")).strip().lower() == track_lower
        ]
        chosen = exact_matches[0] if exact_matches else candidates[0]
        logger.warning(
            "Selected session_key=%s (circuit_short_name=%s, country_name=%s) as the resolved match.",
            chosen.get("session_key"), chosen.get("circuit_short_name"), chosen.get("country_name"),
        )
    else:
        chosen = candidates[0]

    session_key = chosen["session_key"]
    logger.info(
        "Resolved session_key=%s (circuit_short_name=%s, country_name=%s, session_name=%s, date=%s)",
        session_key, chosen.get("circuit_short_name"), chosen.get("country_name"),
        chosen.get("session_name"), chosen.get("date_start"),
    )
    return session_key


def get_drivers(session, base_url, session_key):
    """Return the list of driver_number values present in the given session."""
    url = f"{base_url}/drivers"
    params = {"session_key": session_key}
    response = _request_with_retry(session, url, params=params)
    drivers = response.json()
    driver_numbers = sorted({d["driver_number"] for d in drivers if "driver_number" in d})
    return driver_numbers


def fetch_car_data(session, base_url, session_key, driver_number):
    """Fetch raw car_data samples for a single driver in a session."""
    url = f"{base_url}/car_data"
    params = {"session_key": session_key, "driver_number": driver_number}
    response = _request_with_retry(session, url, params=params)
    return response.json()


def main():
    from quixstreams import Application

    year = os.environ["YEAR"]
    track = os.environ["TRACK"]
    session_name = os.environ["SESSION_NAME"]
    base_url = os.environ.get("OPENF1_BASE_URL", DEFAULT_BASE_URL)

    logger.info("=== OpenF1 Source Job ===")
    logger.info("Year: %s | Track: %s | Session: %s | Base URL: %s", year, track, session_name, base_url)

    http_session = requests.Session()

    session_key = resolve_session_key(http_session, base_url, year, track, session_name)

    app = Application(consumer_group="openf1-source")
    topic = app.topic(os.environ["output"], value_serializer="json")

    driver_numbers = get_drivers(http_session, base_url, session_key)
    logger.info("Found %d drivers for session_key=%s: %s", len(driver_numbers), session_key, driver_numbers)

    total_produced = 0
    with app.get_producer() as producer:
        for driver_number in driver_numbers:
            samples = fetch_car_data(http_session, base_url, session_key, driver_number)
            key = str(driver_number).encode()
            for sample in samples:
                serialized = topic.serialize(key=key, value=sample)
                producer.produce(topic=topic.name, key=serialized.key, value=serialized.value)
            total_produced += len(samples)
            logger.info("driver %s: %d samples produced", driver_number, len(samples))

        producer.flush()

    logger.info("Total samples produced: %d across %d drivers", total_produced, len(driver_numbers))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Job failed: %s", e, exc_info=True)
        sys.exit(1)
