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
    "meetings": "meetings",
    "sessions": "sessions",
    "drivers": "drivers",
}


def make_session() -> requests.Session:
    s = requests.Session()
    if CONFIG_API_TOKEN:
        s.headers.update({"Authorization": f"Bearer {CONFIG_API_TOKEN}"})
    return s


def _build_key(entity_type: str, value: dict) -> str:
    if entity_type == "meetings":
        mk = str(value.get("meeting_key", "unknown"))
        return mk
    elif entity_type == "sessions":
        mk = str(value.get("meeting_key", "unknown"))
        sk = str(value.get("session_key", "unknown"))
        return f"{mk}-{sk}"
    elif entity_type == "drivers":
        mk = str(value.get("meeting_key", "unknown"))
        sk = str(value.get("session_key", "unknown"))
        dn = str(value.get("driver_number", "unknown"))
        return f"{mk}-{sk}-{dn}"
    else:
        return "unknown"


def post_to_config_api(session: requests.Session, config_type: str, config_key: str, value: dict):
    url = f"{CONFIG_API_URL}/api/v1/configurations"
    payload = {
        "metadata": {"type": config_type, "category": config_type, "target_key": config_key},
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
        consumer_group="openf1-metadata-sink-v4",
        auto_offset_reset="earliest",
    )

    meetings_topic = app.topic(os.environ["input_meetings"], value_deserializer="json")
    sessions_topic = app.topic(os.environ["input_sessions"], value_deserializer="json")
    drivers_topic  = app.topic(os.environ["input_drivers"],  value_deserializer="json")

    def make_sink(entity_type: str):
        config_type = _ENTITY_TYPES.get(entity_type, entity_type)

        def transform(value):
            config_key = _build_key(entity_type, value)
            post_to_config_api(session, config_type, config_key, value)
            return value

        return transform

    app.dataframe(meetings_topic).apply(make_sink("meetings"))
    app.dataframe(sessions_topic).apply(make_sink("sessions"))
    app.dataframe(drivers_topic).apply(make_sink("drivers"))

    logger.info("Starting OpenF1 Metadata Sink — POSTing to %s", CONFIG_API_URL)
    app.run()


if __name__ == "__main__":
    main()
