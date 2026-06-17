import json
import logging
import os
import threading
import time

import requests
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Global config cache: {(category, target_key): content_dict}
# Written by the background config-reader thread, read by the SDF enrichment.
_config_cache: dict = {}
_cache_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Pure helper functions (importable and testable without Kafka)
# ---------------------------------------------------------------------------

def parse_lookup_fields(lookup_json: str) -> list:
    """
    Parse LOOKUP_FIELDS_JSON into a list of (output_field, category, source_field) tuples.

    Expected input format::

        {"output_field": "category.source_field", ...}

    Example::

        {"driver_full_name": "drivers.full_name", "year": "sessions.year"}
    """
    raw = json.loads(lookup_json)
    result = []
    for output_field, ref in raw.items():
        parts = ref.split(".", 1)
        if len(parts) != 2:
            logger.warning("Invalid field reference %r — expected 'category.field_name'; skipping", ref)
            continue
        category, source_field = parts
        result.append((output_field, category, source_field))
    return result


def build_lookup_key(category: str, value: dict) -> str:
    """
    Return the config-cache target key for the given category and message.

    - drivers  → "{meeting_key}-{session_key}-{driver_number}"
    - sessions → "{meeting_key}-{session_key}"
    - meetings → "{meeting_key}"
    """
    meeting_key = value.get("meeting_key", "")
    session_key = value.get("session_key", "")
    driver_number = value.get("driver_number", "")

    if category == "drivers":
        return f"{meeting_key}-{session_key}-{driver_number}"
    elif category == "sessions":
        return f"{meeting_key}-{session_key}"
    elif category == "meetings":
        return str(meeting_key)
    else:
        return ""


def enrich_message(value: dict, cache: dict, lookup_fields: list) -> dict:
    """
    Enrich a car-data message using values from the config cache.

    Returns a new dict with discovered fields merged in.  Fields whose config
    entry is absent from the cache are silently omitted — the original message
    is always returned intact.
    """
    result = {**value}
    for output_field, category, source_field in lookup_fields:
        target_key = build_lookup_key(category, value)
        if not target_key:
            continue
        content = cache.get((category, target_key))
        if content is None:
            continue
        field_value = content.get(source_field)
        if field_value is not None:
            result[output_field] = field_value
    return result


# ---------------------------------------------------------------------------
# Config-reader background thread
# ---------------------------------------------------------------------------

def fetch_content(content_url: str, token: str) -> dict:
    """Fetch JSON content from a Quix Configuration Service URL."""
    resp = requests.get(
        content_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def process_config_event(event: dict, cache: dict, cache_lock: threading.Lock) -> None:
    """
    Handle a single config-ui Kafka event: fetch its contentUrl and update cache.

    Expected event shape::

        {
            "event": "created",
            "id": "...",
            "contentUrl": "https://...",
            "metadata": {
                "category": "drivers",
                "target_key": "1218-9157-44",
                ...
            }
        }
    """
    metadata = event.get("metadata", {})
    category = metadata.get("category")
    target_key = metadata.get("target_key")
    content_url = event.get("contentUrl")

    if not all([category, target_key, content_url]):
        return

    token = os.environ.get("Quix__Sdk__Token", "")
    try:
        content = fetch_content(content_url, token)
        with cache_lock:
            cache[(category, target_key)] = content
        logger.debug("Cached config %s/%s", category, target_key)
    except Exception as exc:
        logger.warning("Failed to fetch config %s/%s from %s: %s", category, target_key, content_url, exc)


def start_config_reader(config_topic_name: str, consumer_group: str) -> None:
    """
    Launch a daemon thread that continuously reads the config-ui topic and
    populates the in-memory cache.  The main processing loop is NOT blocked
    by this — cache misses simply leave enrichment fields absent until the
    relevant config event has been consumed.
    """

    def _run() -> None:
        logger.info(
            "Config reader starting — topic=%s consumer_group=%s",
            config_topic_name,
            consumer_group,
        )
        try:
            cfg_app = Application(
                consumer_group=consumer_group,
                auto_offset_reset="earliest",
            )
            cfg_topic = cfg_app.topic(config_topic_name)
            with cfg_app.get_consumer() as consumer:
                consumer.subscribe([cfg_topic.name])
                while True:
                    msg = consumer.poll(timeout=5.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.warning("Config consumer error: %s", msg.error())
                        continue
                    raw = msg.value()
                    if raw is None:
                        continue
                    try:
                        event = json.loads(raw.decode("utf-8"))
                        process_config_event(event, _config_cache, _cache_lock)
                    except Exception as exc:
                        logger.warning("Config event processing error: %s", exc)
        except Exception as exc:
            logger.error("Config reader thread crashed: %s", exc)

    t = threading.Thread(target=_run, name="config-reader", daemon=True)
    t.start()
    logger.info("Config reader thread started")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    data_topic_name = os.environ["DATA_TOPIC"]
    config_topic_name = os.environ["CONFIG_TOPIC"]
    output_topic_name = os.environ["OUTPUT_TOPIC"]
    consumer_group = os.environ["CONSUMER_GROUP_NAME"]
    lookup_fields_json = os.environ["LOOKUP_FIELDS_JSON"]

    lookup_fields = parse_lookup_fields(lookup_fields_json)
    logger.info("Configured %d lookup fields: %s", len(lookup_fields), lookup_fields)

    # Start background config reader (non-blocking)
    start_config_reader(config_topic_name, consumer_group + "-config")

    # Brief warm-up pause: allow the config reader to pre-load entries that
    # are already in the topic before we start emitting enriched messages.
    # Processing starts regardless — this is best-effort, not a hard wait.
    time.sleep(3)

    app = Application(consumer_group=consumer_group, auto_offset_reset="earliest")
    data_topic = app.topic(data_topic_name, key_deserializer="str")
    output_topic = app.topic(output_topic_name)

    # Per-run miss tracking for periodic logging
    counters = {"msgs": 0, "misses": 0}

    def apply_enrichment(value: dict) -> dict:
        counters["msgs"] += 1
        enriched = enrich_message(value, _config_cache, lookup_fields)

        # Check whether any fields were added
        new_keys = set(enriched.keys()) - set(value.keys())
        expected_keys = {field for field, _, _ in lookup_fields}
        missing_keys = expected_keys - new_keys
        if missing_keys:
            counters["misses"] += 1
            if counters["msgs"] % 1000 == 0:
                logger.warning(
                    "Cache-miss summary: %d/%d messages missing enrichment fields; "
                    "example missing: %s",
                    counters["misses"],
                    counters["msgs"],
                    sorted(missing_keys)[:3],
                )

        return enriched

    sdf = app.dataframe(data_topic)
    sdf = sdf.apply(apply_enrichment)
    sdf = sdf.to_topic(output_topic)

    logger.info("Car data enricher running — consuming from '%s'", data_topic_name)
    app.run()


if __name__ == "__main__":
    main()
