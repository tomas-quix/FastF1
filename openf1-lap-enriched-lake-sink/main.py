import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    input_topic    = os.environ["input_topic"]
    table_name     = os.environ.get("TABLE_NAME", "car_telemetry")
    s3_prefix      = os.environ.get("S3_PREFIX", "data-lake/time-series")
    consumer_group = os.environ.get("CONSUMER_GROUP", "openf1-lap-enriched-lake-sink-v1")

    app = Application(consumer_group=consumer_group, auto_offset_reset="earliest")

    car_data_topic = app.topic(input_topic)

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

    # 2. Map lap-enriched fields to hive partition columns
    def add_hive_fields(value):
        value["lap"]          = str(value.get("lap_number") or "0")
        value["racetrack"]    = str(value.get("circuit", "unknown"))
        value["driver"]       = str(value.get("driver_acronym", "unknown"))
        value["year"]         = str(value.get("year", "unknown"))
        value["session_type"] = str(value.get("session_type", "unknown"))
        value["session_name"] = str(value.get("session_name", "unknown"))
        return value

    sdf = sdf.apply(add_hive_fields)

    # 3. Write to QuixLake as Hive-partitioned Parquet
    sdf.sink(sink)

    app.run()


if __name__ == "__main__":
    main()
