from dotenv import load_dotenv
load_dotenv()

import os
import sys
import math
import datetime

import fastf1
import numpy as np
import pandas as pd
from quixstreams import Application


def sanitize(obj):
    """Recursively convert non-JSON-serializable types to JSON-safe equivalents."""
    if isinstance(obj, pd.Timedelta) or isinstance(obj, datetime.timedelta):
        return obj.total_seconds() * 1000  # total milliseconds
    if isinstance(obj, pd.Timestamp) or isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return None if math.isnan(val) else val
    if isinstance(obj, np.ndarray):
        return [sanitize(v) for v in obj.tolist()]
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj


def main():
    import logging

    class TracebackHandler(logging.StreamHandler):
        """Re-emit WARNING+ records with current exc_info so FastF1 silent catches become visible."""
        def emit(self, record):
            if record.levelno >= logging.WARNING and not record.exc_info:
                record.exc_info = sys.exc_info()
            super().emit(record)

    ff1_logger = logging.getLogger("fastf1")
    ff1_logger.handlers.clear()
    ff1_logger.addHandler(TracebackHandler())
    ff1_logger.setLevel(logging.DEBUG)
    ff1_logger.propagate = False

    for handler in logging.root.handlers:
        handler.setLevel(logging.DEBUG)
    logger = logging.getLogger(__name__)

    year = int(os.environ["YEAR"])
    event = os.environ["EVENT"]
    session_type = os.environ["SESSION_TYPE"]

    topic_laps_name = os.environ["output_laps"]
    topic_car_telemetry_name = os.environ["output_car_telemetry"]
    topic_position_name = os.environ["output_position"]
    topic_results_name = os.environ["output_results"]
    topic_weather_name = os.environ["output_weather"]

    print(f"[FastF1 Source] Loading {year} {event} {session_type} ...")

    cache_dir = "/app/cache"
    print(f"[FastF1 Source] Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir, exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    session = fastf1.get_session(year, event, session_type)
    try:
        session.load(laps=True, telemetry=True, weather=True, messages=False)
    except Exception:
        logger.exception("session.load() raised an exception")
        sys.exit(1)

    if session.laps is None or len(session.laps) == 0:
        logger.error("No lap data loaded — session.load() may have failed silently. Check warnings above.")
        sys.exit(1)

    app = Application(
        producer_extra_config={
            "linger.ms": 200,
            "batch.size": 1000000,
        }
    )

    topic_laps = app.topic(topic_laps_name, value_serializer="json")
    topic_car_telemetry = app.topic(topic_car_telemetry_name, value_serializer="json")
    topic_position = app.topic(topic_position_name, value_serializer="json")
    topic_results = app.topic(topic_results_name, value_serializer="json")
    topic_weather = app.topic(topic_weather_name, value_serializer="json")

    count_laps = 0
    count_car_telemetry = 0
    count_position = 0
    count_results = 0
    count_weather = 0

    with app.get_producer() as producer:
        # --- Publish laps ---
        for _, lap in session.laps.iterlaps():
            key = str(lap["Driver"])
            value = sanitize(lap.to_dict())
            producer.produce(
                topic=topic_laps.name,
                key=key,
                value=topic_laps.serialize(key=key, value=value).value,
            )
            count_laps += 1
        producer.flush()
        print(f"[FastF1 Source] Laps published: {count_laps}")

        # --- Publish car telemetry ---
        for driver_num, df in session.car_data.items():
            try:
                abbreviation = session.get_driver(driver_num)["Abbreviation"]
            except Exception:
                abbreviation = str(driver_num)
            driver_count = 0
            try:
                for _, row in df.iterrows():
                    value = sanitize(row.to_dict())
                    producer.produce(
                        topic=topic_car_telemetry.name,
                        key=abbreviation,
                        value=topic_car_telemetry.serialize(key=abbreviation, value=value).value,
                    )
                    driver_count += 1
                    count_car_telemetry += 1
            except Exception as exc:
                print(f"[FastF1 Source] WARNING: Skipping car telemetry for driver {driver_num}: {exc}")
            print(f"[FastF1 Source]   Car telemetry driver {abbreviation}: {driver_count} rows")
        producer.flush()
        print(f"[FastF1 Source] Car telemetry published: {count_car_telemetry}")

        # --- Publish position data ---
        for driver_num, df in session.pos_data.items():
            try:
                abbreviation = session.get_driver(driver_num)["Abbreviation"]
            except Exception:
                abbreviation = str(driver_num)
            driver_count = 0
            try:
                for _, row in df.iterrows():
                    value = sanitize(row.to_dict())
                    producer.produce(
                        topic=topic_position.name,
                        key=abbreviation,
                        value=topic_position.serialize(key=abbreviation, value=value).value,
                    )
                    driver_count += 1
                    count_position += 1
            except Exception as exc:
                print(f"[FastF1 Source] WARNING: Skipping position data for driver {driver_num}: {exc}")
            print(f"[FastF1 Source]   Position driver {abbreviation}: {driver_count} rows")
        producer.flush()
        print(f"[FastF1 Source] Position published: {count_position}")

        # --- Publish session results ---
        for _, row in session.results.iterrows():
            key = str(row["Abbreviation"])
            value = sanitize(row.to_dict())
            producer.produce(
                topic=topic_results.name,
                key=key,
                value=topic_results.serialize(key=key, value=value).value,
            )
            count_results += 1
        producer.flush()
        print(f"[FastF1 Source] Results published: {count_results}")

        # --- Publish weather ---
        for _, row in session.weather_data.iterrows():
            key = "weather"
            value = sanitize(row.to_dict())
            producer.produce(
                topic=topic_weather.name,
                key=key,
                value=topic_weather.serialize(key=key, value=value).value,
            )
            count_weather += 1
        producer.flush()
        print(f"[FastF1 Source] Weather published: {count_weather}")

    print(
        f"\n[FastF1 Source] Done.\n"
        f"Laps:          {count_laps}\n"
        f"Car telemetry: {count_car_telemetry}\n"
        f"Position:      {count_position}\n"
        f"Results:       {count_results}\n"
        f"Weather:       {count_weather}"
    )


if __name__ == "__main__":
    main()
