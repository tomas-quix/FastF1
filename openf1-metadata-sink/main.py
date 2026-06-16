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

_ENTITY_TYPES = {
    "sessions": "openf1/sessions",
    "drivers": "openf1/drivers",
    "stints": "openf1/stints",
    "pit": "openf1/pit",
    "race_control": "openf1/race-control",
    "weather": "openf1/weather",
    "team_radio": "openf1/team-radio",
}


def post_to_config_api(config_type: str, config_key: str, value: dict):
    """POST a single config entry to the Dynamic Configuration Manager API.

    Uses POST /api/v1/configurations with ConfigurationInsert schema:
    - metadata.type: entity prefix (e.g. "openf1/sessions")
    - metadata.target_key: full key path (e.g. "openf1/sessions/9158")
    - content: the full record as a JSON object
    - replace: True so existing entries are versioned rather than rejected
    """
    url = f"{CONFIG_API_URL}/api/v1/configurations"
    payload = {
        "metadata": {
            "type": config_type,
            "target_key": config_key,
        },
        "content": value,
        "replace": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("POSTed %s → %s", config_key, resp.status_code)
    except requests.RequestException as e:
        logger.error("Failed to POST %s: %s", config_key, e)


def make_sink(entity_type: str):
    """Return a transform function that POSTs each record to the Config API."""
    config_type = _ENTITY_TYPES.get(entity_type, f"openf1/{entity_type}")

    def transform(value):
        if entity_type == "sessions":
            config_key = f"openf1/sessions/{value.get('session_key', 'unknown')}"
        elif entity_type == "drivers":
            config_key = f"openf1/drivers/{value.get('driver_number', 'unknown')}"
        elif entity_type == "stints":
            config_key = (
                f"openf1/stints/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('stint_number', 0)}"
            )
        elif entity_type == "pit":
            config_key = (
                f"openf1/pit/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('lap_number', value.get('pit_duration', 'unknown'))}"
            )
        elif entity_type == "race_control":
            config_key = (
                f"openf1/race-control/"
                f"{value.get('lap_number', 'unknown')}/"
                f"{value.get('category', 'unknown')}"
            )
        elif entity_type == "weather":
            config_key = (
                f"openf1/weather/"
                f"{value.get('date', value.get('meeting_key', 'unknown'))}"
            )
        elif entity_type == "team_radio":
            config_key = (
                f"openf1/team-radio/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('date', 'unknown')}"
            )
        else:
            config_key = f"openf1/{entity_type}/unknown"

        post_to_config_api(config_type, config_key, value)
        return value

    return transform


def main():
    app = Application(
        consumer_group="openf1-metadata-sink",
        auto_offset_reset="earliest",
    )

    sessions_topic = app.topic(os.environ["input_sessions"], value_deserializer="json")
    drivers_topic = app.topic(os.environ["input_drivers"], value_deserializer="json")
    stints_topic = app.topic(os.environ["input_stints"], value_deserializer="json")
    pit_topic = app.topic(os.environ["input_pit"], value_deserializer="json")
    race_control_topic = app.topic(
        os.environ["input_race_control"], value_deserializer="json"
    )
    weather_topic = app.topic(os.environ["input_weather"], value_deserializer="json")
    team_radio_topic = app.topic(
        os.environ["input_team_radio"], value_deserializer="json"
    )

    app.dataframe(sessions_topic).apply(make_sink("sessions"))
    app.dataframe(drivers_topic).apply(make_sink("drivers"))
    app.dataframe(stints_topic).apply(make_sink("stints"))
    app.dataframe(pit_topic).apply(make_sink("pit"))
    app.dataframe(race_control_topic).apply(make_sink("race_control"))
    app.dataframe(weather_topic).apply(make_sink("weather"))
    app.dataframe(team_radio_topic).apply(make_sink("team_radio"))

    app.run()


if __name__ == "__main__":
    main()
