import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

from quixstreams import Application


def parse_to_ms(iso_str: str) -> int:
    """Parse an ISO 8601 datetime string to milliseconds since epoch."""
    s = iso_str.replace("+00:00", "").replace("Z", "")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.fromisoformat(s.split(".")[0])
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def car_data_ts(value, headers, timestamp, timestamp_type) -> int:
    """Extract event timestamp from the 'date' field of enriched car data."""
    date_str = value.get("date")
    if date_str:
        return parse_to_ms(date_str)
    return int(timestamp)


def lap_ts(value, headers, timestamp, timestamp_type) -> int:
    """Extract event timestamp from the 'date_start' field of lap data."""
    date_str = value.get("date_start")
    if date_str:
        return parse_to_ms(date_str)
    return int(timestamp)


LAP_TOPIC = os.environ["LAP_TOPIC"]
DATA_TOPIC = os.environ["DATA_TOPIC"]
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "openf1-lap-enricher-v1")

app = Application(consumer_group=CONSUMER_GROUP, auto_offset_reset="earliest")

data_topic = app.topic(DATA_TOPIC, value_deserializer="json",
                       timestamp_extractor=car_data_ts)
lap_topic = app.topic(LAP_TOPIC, value_deserializer="json",
                      timestamp_extractor=lap_ts)
output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

sdf_data = app.dataframe(data_topic)
sdf_laps = app.dataframe(lap_topic)

# join_asof: for each car data point, attach the most recent lap whose
# date_start <= car data date, matched by message key (driver).
# how="left" so car data is never dropped if no lap has arrived yet.
sdf_joined = sdf_data.join_asof(
    right=sdf_laps,
    how="left",
    on_merge="keep-left",
    grace_ms=timedelta(hours=4),
)


def extract_lap(row):
    ts_ms = None
    date_str = row.get("date")
    if date_str:
        try:
            ts_ms = parse_to_ms(date_str)
        except Exception:
            pass
    raw_lap = row.get("lap_number")
    lap_number = int(raw_lap) if raw_lap is not None else None
    return {**row, "lap_number": lap_number, "ts_ms": ts_ms}


sdf_joined = sdf_joined.apply(extract_lap)
sdf_joined = sdf_joined.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
