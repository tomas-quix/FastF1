import os
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()


def build_stats_row(agg_result: dict, driver_number) -> dict:
    """Shape a window aggregation result (start/end/count) plus the
    group_by key (driver_number) into the final flat output message.

    Pure function with no quixstreams dependency so it can be unit
    tested in isolation, without spinning up Kafka.
    """
    return {
        "driver_number": driver_number,
        "window_start": agg_result["start"],
        "window_end": agg_result["end"],
        "count": agg_result["count"],
    }


def main() -> None:
    from quixstreams import Application
    from quixstreams.dataframe.windows import Count

    input_topic_name = os.environ["input"]
    output_topic_name = os.environ["output"]
    consumer_group = os.environ["CONSUMER_GROUP"]

    app = Application(consumer_group=consumer_group, auto_offset_reset="earliest")

    input_topic = app.topic(input_topic_name, value_deserializer="json")
    output_topic = app.topic(output_topic_name, value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Guard against messages missing driver_number to avoid restart loops.
    sdf = sdf.filter(lambda value: value.get("driver_number") is not None)

    # Repartition by driver_number so it becomes the downstream Kafka message key.
    sdf = sdf.group_by("driver_number")

    sdf = sdf.tumbling_window(duration_ms=timedelta(minutes=1)).agg(count=Count()).final()

    sdf["driver_number"] = sdf.apply(lambda value, key, timestamp, headers: key, metadata=True)

    sdf = sdf.apply(lambda value: build_stats_row(value, value["driver_number"]))

    sdf = sdf.to_topic(output_topic)

    app.run()


if __name__ == "__main__":
    main()
