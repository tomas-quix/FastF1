import os
import math
import logging

import fastf1
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("fastf1").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

YEAR = int(os.environ.get("YEAR", "2023"))
_ROUND_RAW = os.environ.get("ROUND", "1")
try:
    ROUND = int(_ROUND_RAW)
except ValueError:
    ROUND = _ROUND_RAW  # allow GP name like "Bahrain"
SESSION_ID = os.environ.get("SESSION", "R")  # R=Race, Q=Quali, FP1/FP2/FP3, S=Sprint

CACHE_DIR = os.environ.get("FASTF1_CACHE_DIR", "/tmp/ff1_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)


def to_native(val):
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        v = float(val)
        return None if math.isnan(v) else v
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, pd.Timedelta):
        return int(val.total_seconds() * 1000)
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def timedelta_to_ms(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timedelta):
        return int(val.total_seconds() * 1000)
    return None


def main():
    logger.info(
        f"Starting FastF1 connector (fastf1==3.8.3): year={YEAR}, round={ROUND}, session={SESSION_ID}"
    )

    app_kwargs = {"consumer_group": "fastf1-connector", "auto_create_topics": True}
    broker_address = os.environ.get("BROKER_ADDRESS")
    if broker_address:
        app_kwargs["broker_address"] = broker_address
    app = Application(**app_kwargs)
    telemetry_topic = app.topic(os.environ["output_telemetry"], value_serializer="json")
    lap_topic = app.topic(os.environ["output_lap_data"], value_serializer="json")

    logger.info("Loading FastF1 session...")
    session = fastf1.get_session(YEAR, ROUND, SESSION_ID)
    session.load(laps=True, telemetry=True, weather=False, messages=False, livedata=None)

    race_name = "unknown"
    try:
        race_name = str(session.event["EventName"])
    except Exception:
        race_name = f"Round {ROUND}"
    logger.info(f"Session loaded: {race_name}")

    laps_df = session.laps
    if laps_df is None or len(laps_df) == 0:
        logger.error("No lap data returned from FastF1 — aborting.")
        return

    drivers = list(session.drivers)
    logger.info(f"Processing {len(drivers)} drivers")

    lap_count = 0
    telemetry_count = 0

    with app.get_producer() as producer:
        for driver_id in drivers:
            driver_laps = laps_df.pick_drivers(driver_id)
            logger.info(f"Driver {driver_id}: {len(driver_laps)} laps")

            for _, lap_row in driver_laps.iterrows():
                lap_num = to_native(lap_row.get("LapNumber"))

                lap_payload = {
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
                    "round": ROUND if isinstance(ROUND, int) else None,
                    "race_name": race_name,
                }
                producer.produce(
                    topic=lap_topic.name,
                    key=str(driver_id),
                    value=lap_topic.serialize(key=str(driver_id), value=lap_payload).value,
                )
                lap_count += 1

                try:
                    tel = lap_row.get_telemetry()
                except Exception as exc:
                    logger.warning(f"  Driver {driver_id} lap {lap_num}: telemetry error: {exc}")
                    continue

                if tel is None or len(tel) == 0:
                    continue

                lap_tel_count = 0
                for _, row in tel.iterrows():
                    tel_payload = {
                        "driver_id": str(driver_id),
                        "lap": int(lap_num) if lap_num is not None else None,
                        "session_time_ms": timedelta_to_ms(row.get("SessionTime")),
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
                        "round": ROUND if isinstance(ROUND, int) else None,
                        "race_name": race_name,
                    }
                    producer.produce(
                        topic=telemetry_topic.name,
                        key=str(driver_id),
                        value=telemetry_topic.serialize(key=str(driver_id), value=tel_payload).value,
                    )
                    telemetry_count += 1
                    lap_tel_count += 1

                logger.info(f"  Driver {driver_id} lap {lap_num}: {lap_tel_count} telemetry rows")

    logger.info(
        f"Done. Produced {lap_count} lap rows and {telemetry_count} telemetry rows "
        f"for {race_name} ({YEAR} round {ROUND}, session {SESSION_ID})."
    )


if __name__ == "__main__":
    main()
