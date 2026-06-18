import os
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv()

from quixstreams import Application

LAP_TOPIC = os.environ["LAP_TOPIC"]
DATA_TOPIC = os.environ["DATA_TOPIC"]
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "openf1-lap-enricher-v1")

app = Application(consumer_group=CONSUMER_GROUP, auto_offset_reset="earliest")

lap_topic = app.topic(LAP_TOPIC, value_deserializer="json")
data_topic = app.topic(DATA_TOPIC, value_deserializer="json")
output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

sdf_data = app.dataframe(data_topic)
sdf_laps = app.dataframe(lap_topic)

# join_asof: for each enriched car data point, attach the most recent lap record
# whose timestamp <= the car data point's timestamp (same message key = same driver).
# Use "left" so car data is never dropped even if no lap record has arrived yet.
sdf_joined = sdf_data.join_asof(
    right=sdf_laps,
    how="left",
    on_merge="keep-left",           # car data fields win on any key collision
    grace_ms=timedelta(hours=4),    # retain lap state for up to 4 hours
)

# Extract just lap_number from the joined right-side fields and keep output clean
def extract_lap(row):
    return {**row, "lap_number": row.get("lap_number")}

sdf_joined = sdf_joined.apply(extract_lap)
sdf_joined = sdf_joined.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
