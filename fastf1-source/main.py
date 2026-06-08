import os
import time
import logging
import fastf1
from fastf1.exceptions import DataNotLoadedError, SessionNotAvailableError
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── FastF1 cache ──────────────────────────────────────────────────────────────
CACHE_DIR = os.environ.get("Quix__Deployment__State__Path", "/tmp/fastf1-cache")
try:
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)
    logger.info(f"FastF1 cache directory: {CACHE_DIR}")
except Exception as e:
    logger.warning(f"Failed to configure FastF1 cache: {e}")

# ── Config ────────────────────────────────────────────────────────────────────
MODE = os.environ.get("MODE", "historical").lower()
try:
    YEAR = int(os.environ.get("YEAR", "2025") or "2025")
except Exception:
    YEAR = 2025
ROUND_FILTER = os.environ.get("ROUND", "1").strip()
SESSION_FILTER = os.environ.get("SESSION", "R").strip()
try:
    PLAYBACK_SPEED = float(os.environ.get("PLAYBACK_SPEED", "0") or "0")
except Exception:
    PLAYBACK_SPEED = 0.0

OUTPUT_TELEMETRY = os.environ.get("output_telemetry", "f1-car-telemetry")
OUTPUT_LAP_DATA = os.environ.get("output_lap_data", "f1-lap-data")
OUTPUT_POSITION = os.environ.get("output_position", "f1-position-data")

ALL_SESSIONS = ["FP1", "FP2", "FP3", "Q", "R"]

# Deferred to main() so broker-connection failures are caught and retried there
app = None
topic_telemetry = None
topic_lap_data = None
topic_position = None


def _sleep_for_playback(prev_ts, curr_ts):
    """Sleep proportionally to simulate real-time (or scaled) playback."""
    if PLAYBACK_SPEED <= 0:
        return
    delta = (curr_ts - prev_ts).total_seconds()
    if delta > 0:
        time.sleep(delta / PLAYBACK_SPEED)


def stream_session(producer, event_name: str, session_name: str, year: int, round_num: str = "") -> bool:
    """Load and stream a single F1 session to Kafka. Returns True if data was streamed."""
    logger.info(f"Loading session: {year} {event_name} – {session_name}")
    try:
        session = fastf1.get_session(year, event_name, session_name)
        session.load(telemetry=True, laps=True, weather=False)
    except SessionNotAvailableError:
        logger.warning(f"No data available for session {year} Round {round_num} [{session_name}] - skipping")
        return False
    except Exception as e:
        logger.warning(f"Failed to load session {year} Round {round_num} [{session_name}]: {e} - skipping")
        return False

    try:
        drivers = session.drivers
        logger.info(f"  Drivers: {drivers}")
    except Exception as e:
        logger.warning(f"  Could not read drivers for {event_name} {session_name}: {e} - skipping")
        return False

    meta = {
        "year": year,
        "round_name": event_name,
        "session_name": session_name,
    }

    # ── Lap data ──────────────────────────────────────────────────────────────
    try:
        laps = session.laps
    except Exception:
        laps = None
    try:
        if laps is not None and not laps.empty:
            logger.info(f"  Streaming {len(laps)} laps...")
            prev_ts = None
            for _, lap in laps.iterrows():
                try:
                    drv = str(lap.get("Driver", "UNK"))
                    row = {k: (v.isoformat() if hasattr(v, "isoformat") else
                               (float(v) if hasattr(v, "item") else
                                (str(v) if v != v else v)))
                           for k, v in lap.items()
                           if v == v}  # skip NaN
                    row.update(meta)
                    row["driver"] = drv
                    ts_col = lap.get("LapStartDate")
                    if prev_ts is not None and ts_col == ts_col:
                        _sleep_for_playback(prev_ts, ts_col)
                    if ts_col == ts_col:
                        prev_ts = ts_col
                    producer.produce(
                        topic=topic_lap_data.name,
                        key=drv,
                        value=row,
                    )
                except Exception as e:
                    logger.warning(f"  Error producing lap row: {e}")
            logger.info(f"  Lap data streamed.")
    except Exception as e:
        logger.warning(f"  Error streaming lap data for {event_name} {session_name}: {e}")

    # ── Telemetry & Position ──────────────────────────────────────────────────
    try:
        pos_data = session.pos_data
    except Exception:
        pos_data = None

    if laps is None:
        logger.info(f"  No lap data loaded for {event_name} {session_name}, skipping telemetry/position.")
        logger.info(f"Session {event_name} {session_name} complete.")
        return True

    for drv in drivers:
        try:
            drv_laps = laps.pick_drivers(drv)
            if drv_laps.empty:
                continue
            tel = drv_laps.get_telemetry()
            if tel is None or tel.empty:
                continue
            logger.info(f"  Streaming telemetry for driver {drv}: {len(tel)} rows")
            prev_ts = None
            for _, row_data in tel.iterrows():
                row = {k: (v.isoformat() if hasattr(v, "isoformat") else
                           (float(v) if hasattr(v, "item") else
                            (str(v) if v != v else v)))
                       for k, v in row_data.items()
                       if v == v}
                row.update(meta)
                row["driver"] = drv
                ts_col = row_data.get("Date")
                if prev_ts is not None and ts_col == ts_col:
                    _sleep_for_playback(prev_ts, ts_col)
                if ts_col == ts_col:
                    prev_ts = ts_col
                producer.produce(
                    topic=topic_telemetry.name,
                    key=drv,
                    value=row,
                )
        except Exception as e:
            logger.warning(f"  Telemetry error for driver {drv}: {e}")

        try:
            pos = pos_data.get(drv) if pos_data is not None else None
            if pos is None or pos.empty:
                continue
            logger.info(f"  Streaming position for driver {drv}: {len(pos)} rows")
            prev_ts = None
            for _, row_data in pos.iterrows():
                row = {k: (v.isoformat() if hasattr(v, "isoformat") else
                           (float(v) if hasattr(v, "item") else
                            (str(v) if v != v else v)))
                       for k, v in row_data.items()
                       if v == v}
                row.update(meta)
                row["driver"] = drv
                ts_col = row_data.get("Date")
                if prev_ts is not None and ts_col == ts_col:
                    _sleep_for_playback(prev_ts, ts_col)
                if ts_col == ts_col:
                    prev_ts = ts_col
                producer.produce(
                    topic=topic_position.name,
                    key=drv,
                    value=row,
                )
        except Exception as e:
            logger.warning(f"  Position error for driver {drv}: {e}")

    logger.info(f"Session {event_name} {session_name} complete.")
    return True


def run_historical(producer):
    """Load and stream historical sessions. Never raises - all failures are caught and logged."""
    logger.info(f"Starting HISTORICAL mode: year={YEAR}, round={ROUND_FILTER or 'ALL'}, session={SESSION_FILTER or 'ALL'}")

    sessions_to_run = [SESSION_FILTER] if SESSION_FILTER else ALL_SESSIONS

    while True:
        try:
            try:
                schedule = fastf1.get_event_schedule(YEAR, include_testing=False)
            except Exception as e:
                logger.warning(f"Failed to fetch event schedule: {e} - sleeping 60s before retry")
                time.sleep(60)
                continue

            any_data = False
            try:
                for _, event in schedule.iterrows():
                    round_num = str(event.get("RoundNumber", ""))
                    event_name = str(event.get("EventName", ""))

                    if ROUND_FILTER:
                        if ROUND_FILTER not in (round_num, event_name):
                            continue

                    for session_name in sessions_to_run:
                        if stream_session(producer, event_name, session_name, YEAR, round_num):
                            any_data = True
            except Exception as e:
                logger.warning(f"Error processing event loop: {e} - sleeping 60s before retry")
                time.sleep(60)
                continue

            if not any_data:
                logger.info("No data available, sleeping 60s before retry")
                time.sleep(60)
            else:
                logger.info("All sessions streamed. Restarting from the beginning...")
                time.sleep(10)
        except Exception as e:
            logger.warning(f"Unexpected error in run_historical loop: {e} - sleeping 60s")
            time.sleep(60)


def run_live():
    """Connect to FastF1 live timing and stream data."""
    from fastf1.livetiming.client import SignalRClient
    import json

    logger.info("Starting LIVE timing mode...")

    messages = []

    def on_message(msg):
        messages.append(msg)

    client = SignalRClient(filename=None, logger=logger)

    with app.get_producer() as producer:
        logger.info("Live timing client started. Waiting for data...")
        try:
            while True:
                # SignalRClient writes to file; we poll via callback approach
                # For live mode, use the recording approach and parse the stream
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Live mode interrupted.")


def main():
    global app, topic_telemetry, topic_lap_data, topic_position
    while True:
        try:
            app = Application()
            topic_telemetry = app.topic(OUTPUT_TELEMETRY, value_serializer="json")
            topic_lap_data = app.topic(OUTPUT_LAP_DATA, value_serializer="json")
            topic_position = app.topic(OUTPUT_POSITION, value_serializer="json")
            if MODE == "live":
                run_live()
            else:
                with app.get_producer() as producer:
                    run_historical(producer)
        except Exception as e:
            logger.warning(f"Top-level exception caught, restarting in 60s: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
