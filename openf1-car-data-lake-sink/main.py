import bisect
import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _drain_topic(topic_name: str, consumer_group: str) -> list:
    """Consume a topic to EOF and return all raw records as dicts."""
    drain_app = Application(
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
    )
    topic = drain_app.topic(topic_name)
    records = []
    with drain_app.get_consumer() as consumer:
        consumer.subscribe([topic.name])
        idle_count = 0
        while idle_count < 3:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                idle_count += 1
                continue
            if msg.error():
                idle_count += 1
                continue
            idle_count = 0
            try:
                records.append(json.loads(msg.value().decode("utf-8")))
            except Exception as e:
                logger.warning("Parse error on %s: %s", topic_name, e)
    return records


def preload_laps(laps_topic_name: str) -> dict:
    """Pre-load all laps; returns {driver_number: [(ts_ms, lap_number), ...]}."""
    logger.info("Pre-loading laps from topic %s...", laps_topic_name)
    lap_index = {}
    for rec in _drain_topic(laps_topic_name, "openf1-lap-preloader-v4"):
        dn = rec.get("driver_number")
        ds = rec.get("date_start")
        ln = rec.get("lap_number")
        if dn is not None and ds and ln is not None:
            try:
                dt = datetime.fromisoformat(ds.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                lap_index.setdefault(dn, []).append((ts_ms, ln))
            except Exception as e:
                logger.warning("Lap parse error: %s", e)
    for dn in lap_index:
        lap_index[dn].sort()
    total = sum(len(v) for v in lap_index.values())
    logger.info("Loaded %d laps across %d drivers", total, len(lap_index))
    return lap_index


def preload_sessions(sessions_topic_name: str) -> dict:
    """Pre-load sessions; returns {'{meeting_key}-{session_key}': session_record}."""
    logger.info("Pre-loading sessions from topic %s...", sessions_topic_name)
    session_index = {}
    for rec in _drain_topic(sessions_topic_name, "openf1-session-preloader-v1"):
        mk = rec.get("meeting_key")
        sk = rec.get("session_key")
        if mk is not None and sk is not None:
            session_index[f"{mk}-{sk}"] = rec
    logger.info("Loaded %d sessions", len(session_index))
    return session_index


def preload_drivers(drivers_topic_name: str) -> dict:
    """Pre-load drivers; returns {'{meeting_key}-{session_key}-{driver_number}': driver_record}."""
    logger.info("Pre-loading drivers from topic %s...", drivers_topic_name)
    driver_index = {}
    for rec in _drain_topic(drivers_topic_name, "openf1-driver-preloader-v1"):
        mk = rec.get("meeting_key")
        sk = rec.get("session_key")
        dn = rec.get("driver_number")
        if mk is not None and sk is not None and dn is not None:
            driver_index[f"{mk}-{sk}-{dn}"] = rec
    logger.info("Loaded %d drivers", len(driver_index))
    return driver_index


def find_lap(lap_index: dict, driver_number, ts_ms: int) -> str:
    laps = lap_index.get(int(driver_number), [])
    if not laps:
        return "0"
    times = [entry[0] for entry in laps]
    idx = bisect.bisect_right(times, ts_ms) - 1
    return str(laps[idx][1]) if idx >= 0 else "0"


def main():
    input_car_data = os.environ["input_car_data"]
    input_sessions = os.environ["input_sessions"]
    input_drivers = os.environ["input_drivers"]
    input_laps = os.environ["input_laps"]
    table_name = os.environ.get("TABLE_NAME", "car_telemetry")
    s3_prefix = os.environ.get("S3_PREFIX", "data-lake/time-series")
    consumer_group = os.environ.get("CONSUMER_GROUP", "openf1-car-data-lake-sink-v1")

    app = Application(consumer_group=consumer_group, auto_offset_reset="earliest")

    # Pre-load all reference data before starting main processing loop.
    # Sessions/drivers topics are small (tens of records) so this completes in seconds.
    lap_index = preload_laps(input_laps)
    session_index = preload_sessions(input_sessions)
    driver_index = preload_drivers(input_drivers)

    car_data_topic = app.topic(input_car_data)

    sink = QuixTSDataLakeSink(
        s3_prefix=s3_prefix,
        table_name=table_name,
        timestamp_column="ts_ms",
        hive_columns=["year", "racetrack", "session_type", "session_name", "driver", "lap"],
    )

    sdf = app.dataframe(topic=car_data_topic)

    # 1. Parse ISO date string to epoch milliseconds
    def add_timestamp(value):
        try:
            dt = datetime.fromisoformat(value["date"].replace("Z", "+00:00"))
            value["ts_ms"] = int(dt.timestamp() * 1000)
        except Exception:
            value["ts_ms"] = 0
        return value

    sdf = sdf.apply(add_timestamp)

    # 2. Inject lap number from pre-loaded in-memory index
    def add_lap(value):
        value["lap"] = find_lap(
            lap_index,
            value.get("driver_number", 0),
            value.get("ts_ms", 0),
        )
        return value

    sdf = sdf.apply(add_lap)

    # 3. Session enrichment from pre-loaded index (year, circuit, session type/name)
    def add_session_fields(value):
        key = f"{value.get('meeting_key')}-{value.get('session_key')}"
        session = session_index.get(key, {})
        value["year"] = session.get("year", 0)
        value["racetrack"] = session.get("circuit_short_name", "unknown")
        value["session_type"] = session.get("session_type", "unknown")
        value["session_name"] = session.get("session_name", "unknown")
        return value

    sdf = sdf.apply(add_session_fields)

    # 4. Driver enrichment from pre-loaded index (driver acronym)
    def add_driver_fields(value):
        key = f"{value.get('meeting_key')}-{value.get('session_key')}-{value.get('driver_number')}"
        driver = driver_index.get(key, {})
        value["driver"] = driver.get("name_acronym", "unknown")
        return value

    sdf = sdf.apply(add_driver_fields)

    # 5. Write to QuixLake as Hive-partitioned Parquet
    sdf.sink(sink)

    app.run()


if __name__ == "__main__":
    main()
