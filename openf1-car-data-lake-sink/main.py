import bisect
import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.dataframe.joins.lookups import QuixConfigurationService
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def preload_laps(broker_conf: dict, laps_topic_name: str) -> dict:
    """
    Consume openf1-laps topic to EOF and build a per-driver sorted lap index.
    Returns: {driver_number (int): [(date_start_ms, lap_number), ...]} sorted by date_start_ms
    """
    from confluent_kafka import Consumer

    logger.info("Pre-loading laps from topic %s...", laps_topic_name)

    conf = {
        **broker_conf,
        "group.id": "openf1-lap-preloader-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }

    consumer = Consumer(conf)
    consumer.subscribe([laps_topic_name])

    lap_index: dict = {}
    idle_polls = 0

    while idle_polls < 5:
        msg = consumer.poll(2.0)
        if msg is None:
            idle_polls += 1
            continue
        if msg.error():
            idle_polls += 1
            continue
        idle_polls = 0
        try:
            rec = json.loads(msg.value().decode("utf-8"))
            dn = rec.get("driver_number")
            ds = rec.get("date_start")
            ln = rec.get("lap_number")
            if dn is not None and ds and ln is not None:
                dt = datetime.fromisoformat(ds.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                lap_index.setdefault(int(dn), []).append((ts_ms, int(ln)))
        except Exception as e:
            logger.warning("Lap parse error: %s", e)

    consumer.close()

    for dn in lap_index:
        lap_index[dn].sort()

    total = sum(len(v) for v in lap_index.values())
    logger.info("Loaded %d laps across %d drivers", total, len(lap_index))
    return lap_index


def find_lap(lap_index: dict, driver_number, ts_ms: int) -> str:
    laps = lap_index.get(int(driver_number), [])
    if not laps:
        return "0"
    times = [entry[0] for entry in laps]
    idx = bisect.bisect_right(times, ts_ms) - 1
    return str(laps[idx][1]) if idx >= 0 else "0"


def main():
    input_car_data = os.environ["input_car_data"]
    input_config = os.environ["input_config"]
    input_laps = os.environ["input_laps"]
    table_name = os.environ.get("TABLE_NAME", "car_telemetry")
    s3_prefix = os.environ.get("S3_PREFIX", "data-lake/time-series")
    consumer_group = os.environ.get("CONSUMER_GROUP", "openf1-car-data-lake-sink-v1")

    app = Application(consumer_group=consumer_group, auto_offset_reset="earliest")

    # Extract broker config for the confluent_kafka lap pre-loader consumer
    broker_conf = app.config.broker_address.as_librdkafka_dict(plaintext_secrets=True)

    # Pre-load all lap data before starting main processing loop
    lap_index = preload_laps(broker_conf, input_laps)

    car_data_topic = app.topic(input_car_data)
    config_topic = app.topic(input_config)

    lookup = QuixConfigurationService(
        topic=config_topic,
        app_config=app.config,
        fallback="default",
    )

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

    # 3. Session enrichment: key = "{meeting_key}-{session_key}"
    sdf = sdf.join_lookup(
        lookup=lookup,
        on=lambda v, k: f"{v['meeting_key']}-{v['session_key']}",
        fields={
            "year": lookup.json_field(
                jsonpath="$.year", type="sessions", default=0
            ),
            "racetrack": lookup.json_field(
                jsonpath="$.circuit_short_name", type="sessions", default="unknown"
            ),
            "session_type": lookup.json_field(
                jsonpath="$.session_type", type="sessions", default="unknown"
            ),
            "session_name": lookup.json_field(
                jsonpath="$.session_name", type="sessions", default="unknown"
            ),
        },
    )

    # 4. Driver enrichment: key = "{meeting_key}-{session_key}-{driver_number}"
    sdf = sdf.join_lookup(
        lookup=lookup,
        on=lambda v, k: f"{v['meeting_key']}-{v['session_key']}-{v['driver_number']}",
        fields={
            "driver": lookup.json_field(
                jsonpath="$.name_acronym", type="drivers", default="unknown"
            ),
        },
    )

    # 5. Write to QuixLake as Hive-partitioned Parquet
    sdf.sink(sink)

    app.run()


if __name__ == "__main__":
    main()
