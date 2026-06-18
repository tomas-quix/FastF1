import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_to_ms(iso_str: str) -> int:
    s = iso_str.replace("+00:00", "").replace("Z", "")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.fromisoformat(s.split(".")[0])
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def prepare(row):
    # Add ts_ms from payload date field (event time, not ingestion time)
    try:
        row["ts_ms"] = parse_to_ms(row["date"])
    except Exception:
        row["ts_ms"] = 0
    # Ensure hive partition columns are strings (QuixLake requirement)
    row["year"]         = str(row.get("year", "unknown"))
    row["circuit"]      = str(row.get("circuit", "unknown"))
    row["session_type"] = str(row.get("session_type", "unknown"))
    row["session_name"] = str(row.get("session_name", "unknown"))
    row["driver_acronym"] = str(row.get("driver_acronym", "unknown"))
    row["lap_number"]   = str(row.get("lap_number") if row.get("lap_number") is not None else "unknown")
    return row


def main():
    input_topic  = os.environ["INPUT_TOPIC"]
    table_name   = os.environ.get("TABLE_NAME", "car_telemetry")
    s3_prefix    = os.environ.get("S3_PREFIX", "data-lake/time-series")
    consumer_grp = os.environ.get("CONSUMER_GROUP", "openf1-lap-enriched-lake-sink-v1")

    app = Application(consumer_group=consumer_grp, auto_offset_reset="earliest")

    topic = app.topic(input_topic, value_deserializer="json")

    sink = QuixTSDataLakeSink(
        s3_prefix=s3_prefix,
        table_name=table_name,
        timestamp_column="ts_ms",
        hive_columns=["year", "circuit", "session_type", "session_name", "driver_acronym", "lap_number"],
    )

    sdf = app.dataframe(topic=topic)
    sdf = sdf.apply(prepare)
    sdf.sink(sink)

    app.run()


if __name__ == "__main__":
    main()
