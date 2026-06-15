import os
import math
import logging
import fastf1
import fastf1.ergast
import fastf1.exceptions
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Suppress noisy FastF1 / urllib3 logs
logging.getLogger("fastf1").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

YEAR = int(os.environ.get("YEAR", "2023"))
ROUND = int(os.environ.get("ROUND", "1"))

# Disable the livetiming mirror so FastF1 doesn't try cloud-blocked endpoints
os.environ["FASTF1_LIVETIMING_MIRROR"] = ""

os.makedirs("/tmp/ff1_cache", exist_ok=True)
fastf1.Cache.enable_cache("/tmp/ff1_cache")


def to_native(val):
    """Convert numpy/pandas scalar types to native Python types."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        v = float(val)
        return None if math.isnan(v) else v
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, pd.Timedelta):
        return int(val.total_seconds() * 1000)
    if isinstance(val, pd.NaT.__class__):
        return None
    return val


def timedelta_to_ms(val):
    """Convert a Timedelta (or NaT/None) to integer milliseconds."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        if pd.isnull(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timedelta):
        return int(val.total_seconds() * 1000)
    return None


def load_via_ergast(year, round_number, producer, lap_topic, telemetry_topic):
    """Fall back to Ergast API for lap data when FastF1 livetiming is unavailable."""
    logger.warning("Falling back to Ergast API for lap/race data (no high-freq telemetry will be produced)")

    e = fastf1.ergast.Ergast()

    # Fetch lap times and race results
    lap_times_response = e.get_lap_times(season=year, round=round_number)
    race_results_response = e.get_race_results(season=year, round=round_number)

    # Extract race name from description DataFrame (has 'raceName' column)
    race_name = f"Round {round_number}"
    if hasattr(race_results_response, "description") and race_results_response.description is not None:
        try:
            desc = race_results_response.description
            if hasattr(desc, "columns") and "raceName" in desc.columns:
                race_name = str(desc["raceName"].iloc[0])
        except Exception:
            pass
    if race_name == f"Round {round_number}" and hasattr(lap_times_response, "description"):
        try:
            desc = lap_times_response.description
            if hasattr(desc, "columns") and "raceName" in desc.columns:
                race_name = str(desc["raceName"].iloc[0])
        except Exception:
            pass

    # Build a final-position map from race results: driverId -> final position
    # get_race_results columns include: driverId, driverCode, position, ...
    position_map = {}
    seen_drivers = set()
    if race_results_response.content:
        results_df = race_results_response.content[0]
        if "driverId" in results_df.columns and "position" in results_df.columns:
            for _, row in results_df.iterrows():
                position_map[str(row["driverId"])] = to_native(row["position"])
                seen_drivers.add(str(row["driverId"]))

    lap_count = 0
    telemetry_count = 0

    if not lap_times_response.content:
        logger.warning("Ergast returned no lap time data")
        return lap_count, telemetry_count, race_name

    for content_df in lap_times_response.content:
        if content_df is None or content_df.empty:
            continue

        # Ergast get_lap_times columns: number (lap#), driverId, position, time (Timedelta)
        for _, lap_row in content_df.iterrows():
            driver_id = str(lap_row.get("driverId", "unknown"))
            # 'number' is the lap number in the Ergast lap times response
            lap_num = to_native(lap_row.get("number"))

            # 'time' is already a pd.Timedelta from the Ergast client
            raw_time = lap_row.get("time")
            lap_time_ms = None
            if isinstance(raw_time, pd.Timedelta):
                lap_time_ms = int(raw_time.total_seconds() * 1000)
            elif isinstance(raw_time, str):
                try:
                    parts = raw_time.split(":")
                    if len(parts) == 2:
                        minutes = int(parts[0])
                        seconds = float(parts[1])
                        lap_time_ms = int((minutes * 60 + seconds) * 1000)
                    else:
                        lap_time_ms = int(float(raw_time) * 1000)
                except (ValueError, IndexError):
                    pass

            # per-lap position from lap times; fall back to final race position
            position = to_native(lap_row.get("position")) or position_map.get(driver_id)

            payload = {
                "driver_id": driver_id,
                "lap": int(lap_num) if lap_num is not None else None,
                "LapTime_ms": lap_time_ms,
                "Sector1Time_ms": None,
                "Sector2Time_ms": None,
                "Sector3Time_ms": None,
                "Compound": None,
                "TyreLife": None,
                "Stint": None,
                "Position": position,
                "IsPersonalBest": None,
                "year": year,
                "round": round_number,
                "race_name": race_name,
                "source": "ergast_fallback",
            }
            producer.produce(
                topic=lap_topic.name,
                key=driver_id,
                value=lap_topic.serialize(key=driver_id, value=payload).value,
            )
            lap_count += 1

    # Produce one placeholder telemetry message per driver so the topic is not empty
    for driver_id in (seen_drivers if seen_drivers else {"unknown"}):
        placeholder = {
            "driver_id": driver_id,
            "source": "ergast_fallback",
            "year": year,
            "round": round_number,
            "race_name": race_name,
            "note": "high-freq telemetry unavailable from cloud",
        }
        producer.produce(
            topic=telemetry_topic.name,
            key=driver_id,
            value=telemetry_topic.serialize(key=driver_id, value=placeholder).value,
        )
        telemetry_count += 1

    logger.info(f"Ergast fallback: produced {lap_count} lap rows, {telemetry_count} telemetry placeholder rows")
    return lap_count, telemetry_count, race_name


def main():
    logger.info(f"Starting FastF1 source: year={YEAR}, round={ROUND}")

    app = Application(consumer_group="fastf1-source")
    telemetry_topic = app.topic(os.environ["output_telemetry"], value_serializer="json")
    lap_topic = app.topic(os.environ["output_lap_data"], value_serializer="json")

    logger.info("Loading FastF1 session (Race)...")
    session = fastf1.get_session(YEAR, ROUND, "R")

    session_loaded = False
    try:
        # livedata=None explicitly tells FastF1 not to use any live data source
        session.load(telemetry=True, laps=True, weather=False, messages=False, livedata=None)
        # session.load() does NOT raise when livetiming is unavailable - it silently
        # returns empty DataFrames. Explicitly check that laps actually loaded.
        try:
            laps_data = session.laps
            if laps_data is None or len(laps_data) == 0:
                raise Exception("Session laps empty after load - livetiming unavailable, using Ergast fallback")
        except fastf1.exceptions.DataNotLoadedError as exc:
            raise Exception(f"Session laps not loaded (DataNotLoadedError): {exc}") from exc
        session_loaded = True
    except Exception as exc:
        logger.warning(f"FastF1 session.load() unavailable: {exc}. Falling back to Ergast.")

    if session_loaded:
        race_name = session.event["EventName"]
        logger.info(f"Session loaded via FastF1: {race_name}")

        telemetry_count = 0
        lap_count = 0

        with app.get_producer() as producer:
            drivers = session.drivers
            logger.info(f"Processing {len(drivers)} drivers")

            for driver_id in drivers:
                driver_laps = session.laps.pick_drivers(driver_id)
                logger.info(f"Driver {driver_id}: {len(driver_laps)} laps")

                # --- Lap data ---
                for _, lap_row in driver_laps.iterrows():
                    lap_num = to_native(lap_row.get("LapNumber"))
                    payload = {
                        "driver_id": str(driver_id),
                        "lap": int(lap_num) if lap_num is not None else None,
                        "LapTime_ms": timedelta_to_ms(lap_row.get("LapTime")),
                        "Sector1Time_ms": timedelta_to_ms(lap_row.get("Sector1Time")),
                        "Sector2Time_ms": timedelta_to_ms(lap_row.get("Sector2Time")),
                        "Sector3Time_ms": timedelta_to_ms(lap_row.get("Sector3Time")),
                        "Compound": to_native(lap_row.get("Compound")),
                        "TyreLife": to_native(lap_row.get("TyreLife")),
                        "Stint": to_native(lap_row.get("Stint")),
                        "Position": to_native(lap_row.get("Position")),
                        "IsPersonalBest": to_native(lap_row.get("IsPersonalBest")),
                        "year": YEAR,
                        "round": ROUND,
                        "race_name": race_name,
                    }
                    producer.produce(
                        topic=lap_topic.name,
                        key=str(driver_id),
                        value=lap_topic.serialize(key=str(driver_id), value=payload).value,
                    )
                    lap_count += 1

                # --- Telemetry data ---
                for _, lap_row in driver_laps.iterrows():
                    lap_num = to_native(lap_row.get("LapNumber"))
                    try:
                        tel = lap_row.get_telemetry()
                    except Exception as exc:
                        logger.warning(f"  Driver {driver_id} lap {lap_num}: telemetry error: {exc}")
                        continue

                    if tel is None or len(tel) == 0:
                        continue

                    lap_tel_count = 0
                    for _, row in tel.iterrows():
                        session_time = row.get("SessionTime")
                        session_time_ms = timedelta_to_ms(session_time)

                        payload = {
                            "driver_id": str(driver_id),
                            "lap": int(lap_num) if lap_num is not None else None,
                            "session_time_ms": session_time_ms,
                            "Speed": to_native(row.get("Speed")),
                            "RPM": to_native(row.get("RPM")),
                            "nGear": to_native(row.get("nGear")),
                            "Throttle": to_native(row.get("Throttle")),
                            "Brake": to_native(row.get("Brake")),
                            "DRS": to_native(row.get("DRS")),
                            "X": to_native(row.get("X")),
                            "Y": to_native(row.get("Y")),
                            "Z": to_native(row.get("Z")),
                            "year": YEAR,
                            "round": ROUND,
                            "race_name": race_name,
                        }
                        producer.produce(
                            topic=telemetry_topic.name,
                            key=str(driver_id),
                            value=telemetry_topic.serialize(key=str(driver_id), value=payload).value,
                        )
                        telemetry_count += 1
                        lap_tel_count += 1

                    logger.info(f"  Driver {driver_id} lap {lap_num}: {lap_tel_count} telemetry rows")

        logger.info(
            f"Done. Produced {lap_count} lap rows and {telemetry_count} telemetry rows "
            f"for {race_name} ({YEAR} round {ROUND})."
        )
    else:
        # Ergast fallback path
        with app.get_producer() as producer:
            lap_count, telemetry_count, race_name = load_via_ergast(
                YEAR, ROUND, producer, lap_topic, telemetry_topic
            )
        logger.info(
            f"Done (Ergast fallback). Produced {lap_count} lap rows and "
            f"{telemetry_count} telemetry placeholder rows "
            f"for {race_name} ({YEAR} round {ROUND})."
        )


if __name__ == "__main__":
    main()
