import os
import math
import logging
import fastf1
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

# Cache directory baked into the Docker image at build time; writable at runtime
# so FastF1's SQLite cache can operate normally.
CACHE_DIR = os.environ.get("FF1_CACHE_DIR", "/app/ff1_cache")

os.makedirs(CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)


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


def main():
    logger.info(f"Starting FastF1 source: year={YEAR}, round={ROUND}")

    app = Application(consumer_group="fastf1-source")
    telemetry_topic = app.topic(os.environ["output_telemetry"], value_serializer="json")
    lap_topic = app.topic(os.environ["output_lap_data"], value_serializer="json")

    logger.info(f"Loading FastF1 session (Race) from cache at {CACHE_DIR}...")
    session = fastf1.get_session(YEAR, ROUND, "R")
    session.load(telemetry=True, laps=True, weather=False, messages=False)

    assert len(session.laps) > 0, (
        f"FastF1 session laps are empty for {YEAR} round {ROUND} — "
        "cache missing or livetiming unreachable at build time"
    )

    race_name = session.event["EventName"]
    logger.info(
        f"Session loaded: {race_name} — "
        f"{len(session.laps)} laps across {len(session.drivers)} drivers"
    )

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


if __name__ == "__main__":
    main()
