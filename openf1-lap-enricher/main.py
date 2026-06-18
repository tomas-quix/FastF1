import os
import json
import time

from dotenv import load_dotenv

load_dotenv()  # no-op in Quix Cloud; reads .env when running locally

from quixstreams import Application
from lap_join import lap_index, process_lap, get_lap_number

LAP_TOPIC = os.environ["LAP_TOPIC"]
DATA_TOPIC = os.environ["DATA_TOPIC"]
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "openf1-lap-enricher-v1")

app = Application(consumer_group=CONSUMER_GROUP, auto_offset_reset="earliest")

lap_topic = app.topic(LAP_TOPIC, value_deserializer="json")
data_topic = app.topic(DATA_TOPIC, value_deserializer="json")
output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")


with app.get_producer() as producer:
    # Step 1: Pre-load all lap data by draining the laps topic
    print("Pre-loading lap data...")
    with app.get_consumer() as lap_consumer:
        lap_consumer.subscribe([lap_topic.name])
        last_msg_time = time.time()
        lap_count = 0
        while True:
            msg = lap_consumer.poll(timeout=2.0)
            if msg is None:
                if time.time() - last_msg_time > 5.0:
                    break
                continue
            if msg.error():
                continue
            last_msg_time = time.time()
            try:
                value = json.loads(msg.value())
                process_lap(value)
                lap_count += 1
            except Exception as e:
                print(f"Error processing lap: {e}")
    print(f"Loaded {lap_count} lap records, {len(lap_index)} driver-session combinations")

    # Step 2: Stream enriched car data and add lap_number
    sdf = app.dataframe(data_topic)

    def enrich_with_lap(row):
        lap_number = get_lap_number(
            row.get("session_key"),
            row.get("driver_number"),
            row.get("date"),
        )
        return {**row, "lap_number": lap_number}

    sdf = sdf.apply(enrich_with_lap)
    sdf = sdf.to_topic(output_topic)

    app.run()
