import json
import logging
import os
from datetime import datetime

import fastf1
import numpy as np
import pandas as pd
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def safe_value(val):
    """Convert pandas NaN/NaT to None for JSON serialization."""
    try:
        if pd.isna(val):
            return None
    except (ValueError, TypeError):
        pass
    if isinstance(val, pd.Timedelta):
        return val.total_seconds() * 1000  # milliseconds
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val


def serialize_value(value):
    return json.dumps(value).encode("utf-8")


def serialize_key(key):
    return key.encode("utf-8") if isinstance(key, str) else key


def produce_results(producer, topic, session, year, gp, session_type):
    """Produce session results to Kafka."""
    results = session.results
    if results is None or results.empty:
        logger.warning("No results data available")
        return 0

    key = f"{year}_{gp}_{session_type}"
    count = 0

    for _, row in results.iterrows():
        payload = {
            "Abbreviation": safe_value(row.get("Abbreviation")),
            "DriverNumber": safe_value(row.get("DriverNumber")),
            "FullName": safe_value(row.get("FullName")),
            "TeamName": safe_value(row.get("TeamName")),
            "Position": safe_value(row.get("Position")),
            "GridPosition": safe_value(row.get("GridPosition")),
            "Status": safe_value(row.get("Status")),
            "Points": safe_value(row.get("Points")),
            "Q1": safe_value(row.get("Q1")),
            "Q2": safe_value(row.get("Q2")),
            "Q3": safe_value(row.get("Q3")),
            "FastestLapTime": safe_value(row.get("FastestLapTime")),
            "BroadcastName": safe_value(row.get("BroadcastName")),
            "TeamColor": safe_value(row.get("TeamColor")),
            "HeadshotUrl": safe_value(row.get("HeadshotUrl")),
            "CountryCode": safe_value(row.get("CountryCode")),
            "ClassifiedPosition": safe_value(row.get("ClassifiedPosition")),
        }
        producer.produce(
            topic=topic.name,
            key=serialize_key(key),
            value=serialize_value(payload),
        )
        count += 1

    logger.info("Produced %d results rows", count)
    return count


def produce_laps(producer, topic, session):
    """Produce lap data to Kafka."""
    laps = session.laps
    if laps is None or laps.empty:
        logger.warning("No laps data available")
        return 0

    count = 0

    for _, row in laps.iterrows():
        driver = safe_value(row.get("Driver"))
        key = driver if driver else "UNKNOWN"

        payload = {
            "Driver": driver,
            "DriverNumber": safe_value(row.get("DriverNumber")),
            "LapNumber": safe_value(row.get("LapNumber")),
            "LapTime": safe_value(row.get("LapTime")),
            "Sector1Time": safe_value(row.get("Sector1Time")),
            "Sector2Time": safe_value(row.get("Sector2Time")),
            "Sector3Time": safe_value(row.get("Sector3Time")),
            "SpeedI1": safe_value(row.get("SpeedI1")),
            "SpeedI2": safe_value(row.get("SpeedI2")),
            "SpeedFL": safe_value(row.get("SpeedFL")),
            "SpeedST": safe_value(row.get("SpeedST")),
            "Compound": safe_value(row.get("Compound")),
            "TyreLife": safe_value(row.get("TyreLife")),
            "Stint": safe_value(row.get("Stint")),
            "PitInTime": safe_value(row.get("PitInTime")),
            "PitOutTime": safe_value(row.get("PitOutTime")),
            "Position": safe_value(row.get("Position")),
            "TrackStatus": safe_value(row.get("TrackStatus")),
            "Deleted": safe_value(row.get("Deleted")),
            "DeletedReason": safe_value(row.get("DeletedReason")),
            "FreshTyre": safe_value(row.get("FreshTyre")),
            "IsPersonalBest": safe_value(row.get("IsPersonalBest")),
            "Team": safe_value(row.get("Team")),
            "LapStartDate": safe_value(row.get("LapStartDate")),
        }
        producer.produce(
            topic=topic.name,
            key=serialize_key(key),
            value=serialize_value(payload),
        )
        count += 1

    logger.info("Produced %d lap rows", count)
    return count


def produce_weather(producer, topic, session, year, gp, session_type):
    """Produce weather data to Kafka."""
    weather = session.weather_data
    if weather is None or weather.empty:
        logger.warning("No weather data available")
        return 0

    key = f"{year}_{gp}_{session_type}"
    count = 0

    for _, row in weather.iterrows():
        payload = {
            "Time": safe_value(row.get("Time")),
            "AirTemp": safe_value(row.get("AirTemp")),
            "TrackTemp": safe_value(row.get("TrackTemp")),
            "Humidity": safe_value(row.get("Humidity")),
            "Pressure": safe_value(row.get("Pressure")),
            "WindDirection": safe_value(row.get("WindDirection")),
            "WindSpeed": safe_value(row.get("WindSpeed")),
            "Rainfall": safe_value(row.get("Rainfall")),
        }
        producer.produce(
            topic=topic.name,
            key=serialize_key(key),
            value=serialize_value(payload),
        )
        count += 1

    logger.info("Produced %d weather rows", count)
    return count


def produce_telemetry(producer, topic, session):
    """Produce telemetry data to Kafka for each driver."""
    laps = session.laps
    if laps is None or laps.empty:
        logger.warning("No laps data available for telemetry")
        return 0

    drivers = laps["Driver"].unique()
    total_count = 0

    for driver_abbr in drivers:
        try:
            driver_laps = session.laps.pick_driver(driver_abbr)
            telemetry = driver_laps.get_telemetry()
        except Exception as e:
            logger.warning("No telemetry for driver %s: %s", driver_abbr, e)
            continue

        if telemetry is None or telemetry.empty:
            logger.warning("Empty telemetry for driver %s", driver_abbr)
            continue

        count = 0
        for _, row in telemetry.iterrows():
            payload = {
                "Driver": driver_abbr,
                "SessionTime": safe_value(row.get("SessionTime")),
                "Date": safe_value(row.get("Date")),
                "Speed": safe_value(row.get("Speed")),
                "Throttle": safe_value(row.get("Throttle")),
                "Brake": safe_value(row.get("Brake")),
                "nGear": safe_value(row.get("nGear")),
                "RPM": safe_value(row.get("RPM")),
                "DRS": safe_value(row.get("DRS")),
                "X": safe_value(row.get("X")),
                "Y": safe_value(row.get("Y")),
                "Z": safe_value(row.get("Z")),
                "Distance": safe_value(row.get("Distance")),
                "RelativeDistance": safe_value(row.get("RelativeDistance")),
                "Source": safe_value(row.get("Source")),
                "Time": safe_value(row.get("Time")),
            }
            producer.produce(
                topic=topic.name,
                key=serialize_key(driver_abbr),
                value=serialize_value(payload),
            )
            count += 1

        logger.info("Producing telemetry for %s: %d rows", driver_abbr, count)
        total_count += count

    return total_count


def main():
    # Configuration
    year = int(os.environ["F1_YEAR"])
    gp = os.environ["F1_GRAND_PRIX"]
    session_type = os.environ["F1_SESSION"]

    logger.info("Loading FastF1 session: %d %s %s", year, gp, session_type)

    # Set up FastF1 cache
    cache_path = os.environ.get("Quix__Deployment__State__Path", "/tmp/fastf1_cache")
    os.makedirs(cache_path, exist_ok=True)
    fastf1.Cache.enable_cache(cache_path)

    # Load session data
    session = fastf1.get_session(year, gp, session_type)
    session.load(telemetry=True, weather=True, messages=False)
    logger.info("Session loaded successfully")

    # Create Quix Streams app and topics
    app = Application()
    topic_telemetry = app.topic(os.environ["output_telemetry"], value_serializer="json")
    topic_laps = app.topic(os.environ["output_laps"], value_serializer="json")
    topic_weather = app.topic(os.environ["output_weather"], value_serializer="json")
    topic_results = app.topic(os.environ["output_results"], value_serializer="json")

    # Produce all data
    with app.get_producer() as producer:
        results_count = produce_results(
            producer, topic_results, session, year, gp, session_type
        )
        laps_count = produce_laps(producer, topic_laps, session)
        weather_count = produce_weather(
            producer, topic_weather, session, year, gp, session_type
        )
        telemetry_count = produce_telemetry(producer, topic_telemetry, session)

    logger.info(
        "Import complete:\n"
        "  Results: %d rows → %s\n"
        "  Laps: %d rows → %s\n"
        "  Weather: %d rows → %s\n"
        "  Telemetry: %d rows → %s",
        results_count,
        topic_results.name,
        laps_count,
        topic_laps.name,
        weather_count,
        topic_weather.name,
        telemetry_count,
        topic_telemetry.name,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error in FastF1 source")
        raise
