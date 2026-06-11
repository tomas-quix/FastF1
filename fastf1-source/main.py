import os
import time
import logging
import requests
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

YEAR = os.environ.get("YEAR", "2023")
ROUND = os.environ.get("ROUND", "1")
OUTPUT_TELEMETRY = os.environ.get("output_telemetry", "f1-car-telemetry")
OUTPUT_LAP_DATA = os.environ.get("output_lap_data", "f1-lap-data")
OUTPUT_POSITION = os.environ.get("output_position", "f1-position-data")

BASE_URL = "https://api.jolpi.ca/ergast/f1"

app = Application()
topic_telemetry = app.topic(OUTPUT_TELEMETRY, value_serializer="json")
topic_lap_data = app.topic(OUTPUT_LAP_DATA, value_serializer="json")
topic_position = app.topic(OUTPUT_POSITION, value_serializer="json")


def fetch_json(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def stream_race_results(producer, year, round_num):
    """Fetch race results and produce to lap data topic."""
    url = f"{BASE_URL}/{year}/{round_num}/results.json?limit=100"
    logger.info(f"Fetching race results: {url}")
    data = fetch_json(url)
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        logger.warning(f"No race results found for {year} round {round_num}")
        return 0
    race = races[0]
    race_name = race.get("raceName", "Unknown")
    results = race.get("Results", [])
    count = 0
    for result in results:
        driver = result.get("Driver", {})
        constructor = result.get("Constructor", {})
        msg = {
            "year": int(year),
            "round": int(round_num),
            "race_name": race_name,
            "driver_id": driver.get("driverId"),
            "driver_code": driver.get("code"),
            "driver_name": f"{driver.get('givenName')} {driver.get('familyName')}",
            "constructor": constructor.get("name"),
            "grid": int(result.get("grid", 0)),
            "position": result.get("position"),
            "status": result.get("status"),
            "points": float(result.get("points", 0)),
            "laps": int(result.get("laps", 0)),
            "fastest_lap_rank": result.get("FastestLap", {}).get("rank"),
            "fastest_lap_time": result.get("FastestLap", {}).get("Time", {}).get("time"),
            "fastest_lap_speed": result.get("FastestLap", {}).get("AverageSpeed", {}).get("speed"),
        }
        msg_serialized = topic_lap_data.serialize(key=driver.get("driverId", "unknown"), value=msg)
        producer.produce(topic=topic_lap_data.name, key=msg_serialized.key, value=msg_serialized.value)
        count += 1
    logger.info(f"Produced {count} race result rows to {topic_lap_data.name}")
    return count


def stream_lap_times(producer, year, round_num):
    """Fetch detailed lap times and produce to telemetry topic."""
    offset = 0
    limit = 100
    total = 0
    while True:
        url = f"{BASE_URL}/{year}/{round_num}/laps.json?limit={limit}&offset={offset}"
        logger.info(f"Fetching lap times (offset={offset}): {url}")
        data = fetch_json(url)
        mr = data.get("MRData", {})
        total_available = int(mr.get("total", 0))
        races = mr.get("RaceTable", {}).get("Races", [])
        if not races:
            break
        laps = races[0].get("Laps", [])
        if not laps:
            break
        race_name = races[0].get("raceName", "Unknown")
        for lap in laps:
            lap_number = int(lap.get("number", 0))
            for timing in lap.get("Timings", []):
                msg = {
                    "year": int(year),
                    "round": int(round_num),
                    "race_name": race_name,
                    "lap": lap_number,
                    "driver_id": timing.get("driverId"),
                    "position": int(timing.get("position", 0)),
                    "lap_time": timing.get("time"),
                }
                msg_serialized = topic_telemetry.serialize(key=timing.get("driverId", "unknown"), value=msg)
                producer.produce(topic=topic_telemetry.name, key=msg_serialized.key, value=msg_serialized.value)
                total += 1
        offset += limit
        if offset >= total_available:
            break
        time.sleep(0.2)  # be polite to the API
    logger.info(f"Produced {total} lap time rows to {topic_telemetry.name}")
    return total


def stream_pit_stops(producer, year, round_num):
    """Fetch pit stop data and produce to position topic."""
    url = f"{BASE_URL}/{year}/{round_num}/pitstops.json?limit=100"
    logger.info(f"Fetching pit stops: {url}")
    data = fetch_json(url)
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        logger.warning(f"No pit stop data for {year} round {round_num}")
        return 0
    race = races[0]
    race_name = race.get("raceName", "Unknown")
    pit_stops = race.get("PitStops", [])
    count = 0
    for stop in pit_stops:
        msg = {
            "year": int(year),
            "round": int(round_num),
            "race_name": race_name,
            "driver_id": stop.get("driverId"),
            "stop": int(stop.get("stop", 0)),
            "lap": int(stop.get("lap", 0)),
            "time": stop.get("time"),
            "duration": stop.get("duration"),
        }
        msg_serialized = topic_position.serialize(key=stop.get("driverId", "unknown"), value=msg)
        producer.produce(topic=topic_position.name, key=msg_serialized.key, value=msg_serialized.value)
        count += 1
    logger.info(f"Produced {count} pit stop rows to {topic_position.name}")
    return count


def main():
    logger.info(f"Starting F1 data source: year={YEAR}, round={ROUND}")
    with app.get_producer() as producer:
        while True:
            try:
                total = 0
                total += stream_race_results(producer, YEAR, ROUND)
                total += stream_lap_times(producer, YEAR, ROUND)
                total += stream_pit_stops(producer, YEAR, ROUND)
                logger.info(f"Cycle complete: {total} total messages produced. Sleeping 60s...")
                time.sleep(60)
            except Exception:
                logger.exception("Error in main loop, retrying in 30s...")
                time.sleep(30)


if __name__ == "__main__":
    main()
