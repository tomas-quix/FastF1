"""
Config Enricher — subscribes to 8 raw OpenF1 telemetry topics and a
pipeline-config topic, then enriches each telemetry row with driver,
session, and meeting metadata from the config store before writing to
the corresponding enriched-* output topic.

Config store keys (set by Metadata Loader via Dynamic Configuration Manager):
  driver/<driver_number>   → full driver record
  session/<session_key>    → full session record
  meeting/<meeting_key>    → full meeting record
"""

import os
import time
import logging
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Shared in-memory config store — populated by the CONFIG_TOPIC SDF
_config_store: dict[str, dict] = {}
_last_config_update: float = 0.0

# Static session/meeting keys from deployment config (fallback when not in row)
SESSION_KEY = os.environ.get("SESSION_KEY", "")
MEETING_KEY = os.environ.get("MEETING_KEY", "")


# ---------------------------------------------------------------------------
# Config store update
# ---------------------------------------------------------------------------

def _handle_config(value: dict, key, timestamp, headers) -> dict:
    """Update the in-memory config store from a CONFIG_TOPIC message."""
    global _last_config_update
    if key:
        str_key = key.decode() if isinstance(key, bytes) else str(key)
        _config_store[str_key] = value
        _last_config_update = time.time()
        logger.debug("Config store updated: %s", str_key)
    return value


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich_row(row: dict, driver_number, session_key, meeting_key) -> dict:
    """
    Enrich a telemetry row with driver, session, and meeting metadata.

    Returns a new dict (does not mutate the input).
    Always stamps _config_version and _config_age_ms.
    Sets _config_missing=True when driver config is absent.
    """
    result = dict(row)
    now = time.time()
    result["_config_version"] = now
    result["_config_age_ms"] = (
        int((now - _last_config_update) * 1000) if _last_config_update else 0
    )

    # Driver enrichment
    driver_cfg = (
        _config_store.get(f"driver/{driver_number}")
        if driver_number is not None
        else None
    )
    if driver_cfg is None:
        result["_config_missing"] = True
    else:
        result["driver_full_name"] = driver_cfg.get("full_name")
        result["driver_team_name"] = driver_cfg.get("team_name")
        result["driver_country_code"] = driver_cfg.get("country_code")

    # Session enrichment (use row value if present, else env var)
    effective_session = session_key or SESSION_KEY
    session_cfg = (
        _config_store.get(f"session/{effective_session}")
        if effective_session
        else None
    )
    if session_cfg:
        result["session_name"] = session_cfg.get("session_name")
        result["session_type"] = session_cfg.get("session_type")

    # Meeting enrichment (use row value if present, else env var)
    effective_meeting = meeting_key or MEETING_KEY
    meeting_cfg = (
        _config_store.get(f"meeting/{effective_meeting}")
        if effective_meeting
        else None
    )
    if meeting_cfg:
        result["meeting_name"] = meeting_cfg.get("meeting_name")
        result["circuit_short_name"] = meeting_cfg.get("circuit_short_name")

    return result


def _make_enrich_fn():
    """Return a closure capturing SESSION_KEY / MEETING_KEY at startup."""
    sk = SESSION_KEY
    mk = MEETING_KEY

    def _enrich(row: dict) -> dict:
        return enrich_row(
            row,
            driver_number=row.get("driver_number"),
            session_key=row.get("session_key") or sk,
            meeting_key=row.get("meeting_key") or mk,
        )

    return _enrich


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

def main():
    enrich = _make_enrich_fn()

    app = Application(
        consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "config-enricher-v1"),
        auto_offset_reset="earliest",
    )

    # --- Config topic (key = config key string) ---
    config_topic = app.topic(
        os.environ["CONFIG_TOPIC"],
        key_deserializer="str",
        value_deserializer="json",
    )

    # --- Raw input topics ---
    t_car_data  = app.topic(os.environ["input_car_data"],   value_deserializer="json")
    t_laps      = app.topic(os.environ["input_laps"],       value_deserializer="json")
    t_intervals = app.topic(os.environ["input_intervals"],  value_deserializer="json")
    t_location  = app.topic(os.environ["input_location"],   value_deserializer="json")
    t_pit       = app.topic(os.environ["input_pit"],        value_deserializer="json")
    t_position  = app.topic(os.environ["input_position"],   value_deserializer="json")
    t_stints    = app.topic(os.environ["input_stints"],     value_deserializer="json")
    t_team_radio = app.topic(os.environ["input_team_radio"], value_deserializer="json")

    # --- Enriched output topics ---
    out_car_data   = app.topic(os.environ["output_car_data"],   value_serializer="json")
    out_laps       = app.topic(os.environ["output_laps"],       value_serializer="json")
    out_intervals  = app.topic(os.environ["output_intervals"],  value_serializer="json")
    out_location   = app.topic(os.environ["output_location"],   value_serializer="json")
    out_pit        = app.topic(os.environ["output_pit"],        value_serializer="json")
    out_position   = app.topic(os.environ["output_position"],   value_serializer="json")
    out_stints     = app.topic(os.environ["output_stints"],     value_serializer="json")
    out_team_radio = app.topic(os.environ["output_team_radio"], value_serializer="json")

    # Config SDF — updates the in-memory store, no output topic
    app.dataframe(config_topic).update(_handle_config, metadata=True)

    # Telemetry SDFs — enrich and route to enriched topics
    app.dataframe(t_car_data).apply(enrich).to_topic(out_car_data)
    app.dataframe(t_laps).apply(enrich).to_topic(out_laps)
    app.dataframe(t_intervals).apply(enrich).to_topic(out_intervals)
    app.dataframe(t_location).apply(enrich).to_topic(out_location)
    app.dataframe(t_pit).apply(enrich).to_topic(out_pit)
    app.dataframe(t_position).apply(enrich).to_topic(out_position)
    app.dataframe(t_stints).apply(enrich).to_topic(out_stints)
    app.dataframe(t_team_radio).apply(enrich).to_topic(out_team_radio)

    app.run()


if __name__ == "__main__":
    main()
