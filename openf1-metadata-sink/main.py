import os
import logging
import requests
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONFIG_API_URL = os.environ.get(
    "CONFIG_API_URL",
    "https://config-api-svc-quixdev-fastf1-dev.deployments-dev.quix.io",
).rstrip("/")

CONFIG_API_TOKEN = os.environ.get("Quix__Sdk__Token", "")

_ENTITY_TYPES = {
    "sessions": "openf1/sessions",
    "drivers": "openf1/drivers",
    "stints": "openf1/stints",
    "pit": "openf1/pit",
    "race_control": "openf1/race-control",
    "weather": "openf1/weather",
    "team_radio": "openf1/team-radio",
}


def make_session() -> requests.Session:
    s = requests.Session()
    if CONFIG_API_TOKEN:
        s.headers.update({"Authorization": f"Bearer {CONFIG_API_TOKEN}"})
    return s


def _build_key(entity_type: str, value: dict) -> str:
    if entity_type == "sessions":
        return f"openf1/sessions/{value.get('session_key', 'unknown')}"
    elif entity_type == "drivers":
        return f"openf1/drivers/{value.get('driver_number', 'unknown')}"
    elif entity_type == "stints":
        return (
            f"openf1/stints/"
            f"{value.get('driver_number', 'unknown')}/"
            f"{value.get('stint_number', 0)}"
        )
    elif entity_type == "pit":
        return (
            f"openf1/pit/"
            f"{value.get('driver_number', 'unknown')}/"
            f"{value.get('lap_number', value.get('pit_duration', 'unknown'))}"
        )
    elif entity_type == "race_control":
        return (
            f"openf1/race-control/"
            f"{value.get('lap_number', 'unknown')}/"
            f"{value.get('category', 'unknown')}"
        )
    elif entity_type == "weather":
        return f"openf1/weather/{value.get('date', value.get('meeting_key', 'unknown'))}"
    elif entity_type == "team_radio":
        return (
            f"openf1/team-radio/"
            f"{value.get('driver_number', 'unknown')}/"
            f"{value.get('date', 'unknown')}"
        )
    else:
        return f"openf1/{entity_type}/unknown"


def post_to_config_api(session: requests.Session, config_type: str, config_key: str, value: dict):
    url = f"{CONFIG_API_URL}/api/v1/configurations"
    payload = {
        "metadata": {"type": config_type, "target_key": config_key},
        "content": value,
        "replace": True,
    }
    try:
        resp = session.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("POSTed %s → %s", config_key, resp.status_code)
    except requests.HTTPError as e:
        logger.error(
            "HTTP error posting %s: %s — response: %s",
            config_key,
            e,
            e.response.text if e.response else "",
        )
        raise
    except requests.RequestException as e:
        logger.error("Request error posting %s: %s", config_key, e)
        raise


def main():
    session = make_session()

    # Startup probe
    try:
        probe = session.get(f"{CONFIG_API_URL}/api/v1/configurations", timeout=5)
        logger.info("Config API probe: %s", probe.status_code)
        if probe.status_code == 403:
            logger.error("Config API returned 403 — check Quix__Sdk__Token is set correctly")
    except Exception as e:
        logger.warning("Config API probe failed: %s", e)

    app = Application(
        consumer_group="openf1-metadata-sink-v3",
        auto_offset_reset="earliest",
    )

    sessions_topic     = app.topic(os.environ["input_sessions"],     value_deserializer="json")
    drivers_topic      = app.topic(os.environ["input_drivers"],      value_deserializer="json")
    stints_topic       = app.topic(os.environ["input_stints"],       value_deserializer="json")
    pit_topic          = app.topic(os.environ["input_pit"],          value_deserializer="json")
    race_control_topic = app.topic(os.environ["input_race_control"], value_deserializer="json")
    weather_topic      = app.topic(os.environ["input_weather"],      value_deserializer="json")
    team_radio_topic   = app.topic(os.environ["input_team_radio"],   value_deserializer="json")

    def make_sink(entity_type: str):
        config_type = _ENTITY_TYPES.get(entity_type, f"openf1/{entity_type}")

        def transform(value):
            config_key = _build_key(entity_type, value)
            post_to_config_api(session, config_type, config_key, value)
            return value

        return transform

    app.dataframe(sessions_topic).apply(make_sink("sessions"))
    app.dataframe(drivers_topic).apply(make_sink("drivers"))
    app.dataframe(stints_topic).apply(make_sink("stints"))
    app.dataframe(pit_topic).apply(make_sink("pit"))
    app.dataframe(race_control_topic).apply(make_sink("race_control"))
    app.dataframe(weather_topic).apply(make_sink("weather"))
    app.dataframe(team_radio_topic).apply(make_sink("team_radio"))

    logger.info("Starting OpenF1 Metadata Sink — POSTing to %s", CONFIG_API_URL)
    app.run()


if __name__ == "__main__":
    main()
