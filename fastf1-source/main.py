import os
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Suppress fastf1 internal debug spam
import fastf1
fastf1.set_log_level('WARNING')

def main():
    while True:
        try:
            _run()
        except Exception:
            logger.exception("Unhandled exception - restarting in 60s")
            time.sleep(60)

def _run():
    from dotenv import load_dotenv
    load_dotenv()

    import fastf1
    from quixstreams import Application

    fastf1.set_log_level('WARNING')

    CACHE_DIR = os.environ.get("Quix__Deployment__State__Path", "/tmp/fastf1-cache")
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)

    MODE = os.environ.get("MODE", "historical").lower()
    YEAR = int(os.environ.get("YEAR", "2023") or "2023")
    ROUND_FILTER = os.environ.get("ROUND", "1").strip()
    SESSION_FILTER = os.environ.get("SESSION", "R").strip()
    PLAYBACK_SPEED = float(os.environ.get("PLAYBACK_SPEED", "0") or "0")

    OUTPUT_TELEMETRY = os.environ.get("output_telemetry", "f1-car-telemetry")
    OUTPUT_LAP_DATA = os.environ.get("output_lap_data", "f1-lap-data")
    OUTPUT_POSITION = os.environ.get("output_position", "f1-position-data")

    app = Application()
    topic_telemetry = app.topic(OUTPUT_TELEMETRY, value_serializer="json")
    topic_lap_data = app.topic(OUTPUT_LAP_DATA, value_serializer="json")
    topic_position = app.topic(OUTPUT_POSITION, value_serializer="json")

    logger.info(f"Starting: year={YEAR}, round={ROUND_FILTER}, session={SESSION_FILTER}")

    with app.get_producer() as producer:
        while True:
            produced = _run_cycle(producer, topic_telemetry, topic_lap_data, topic_position,
                                  YEAR, ROUND_FILTER, SESSION_FILTER, PLAYBACK_SPEED)
            if produced:
                logger.info("Cycle complete, restarting in 10s...")
                time.sleep(10)
            else:
                logger.info("No data produced, sleeping 60s before retry...")
                time.sleep(60)

def _run_cycle(producer, topic_telemetry, topic_lap_data, topic_position,
               year, round_filter, session_filter, playback_speed):
    import fastf1

    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
    except Exception as e:
        logger.warning(f"Failed to get event schedule for {year}: {e}")
        return False

    produced_any = False

    for _, event in schedule.iterrows():
        round_num = str(event.get("RoundNumber", ""))
        event_name = str(event.get("EventName", ""))

        if round_filter and round_filter not in (round_num, event_name):
            continue

        sessions = [session_filter] if session_filter else ["FP1", "FP2", "FP3", "Q", "R"]
        for sess_name in sessions:
            try:
                result = _stream_session(producer, topic_telemetry, topic_lap_data, topic_position,
                                         year, event_name, sess_name, round_num, playback_speed)
                if result:
                    produced_any = True
            except Exception as e:
                logger.warning(f"Error streaming {year} {event_name} {sess_name}: {e}")

    return produced_any

def _stream_session(producer, topic_telemetry, topic_lap_data, topic_position,
                    year, event_name, sess_name, round_num, playback_speed):
    import fastf1

    logger.info(f"Loading: {year} {event_name} {sess_name}")
    try:
        session = fastf1.get_session(year, event_name, sess_name)
        session.load(laps=True, telemetry=True, weather=False, messages=False)
    except Exception as e:
        logger.warning(f"Cannot load {year} {event_name} {sess_name}: {e}")
        return False

    meta = {"year": year, "event": event_name, "session": sess_name}
    count = 0

    # Lap data
    try:
        laps = session.laps
        if laps is not None and not laps.empty:
            for _, lap in laps.iterrows():
                row = _serialize_row(dict(lap))
                row.update(meta)
                producer.produce(topic=topic_lap_data.name, key=str(row.get("Driver", "UNK")), value=row)
                count += 1
            logger.info(f"  Produced {len(laps)} lap rows")
    except Exception as e:
        logger.warning(f"  Lap data error: {e}")

    # Telemetry per driver
    try:
        drivers = session.drivers
        laps = session.laps
        for drv in drivers:
            try:
                drv_laps = laps.pick_drivers(drv)
                if drv_laps.empty:
                    continue
                tel = drv_laps.get_telemetry()
                if tel is None or tel.empty:
                    continue
                for _, row_data in tel.iterrows():
                    row = _serialize_row(dict(row_data))
                    row.update(meta)
                    row["driver"] = drv
                    producer.produce(topic=topic_telemetry.name, key=str(drv), value=row)
                    count += 1
                logger.info(f"  Produced {len(tel)} telemetry rows for driver {drv}")
            except Exception as e:
                logger.warning(f"  Telemetry error for driver {drv}: {e}")
    except Exception as e:
        logger.warning(f"  Telemetry loop error: {e}")

    # Position data
    try:
        pos_data = session.pos_data
        if pos_data:
            for drv, pos in pos_data.items():
                if pos is None or pos.empty:
                    continue
                for _, row_data in pos.iterrows():
                    row = _serialize_row(dict(row_data))
                    row.update(meta)
                    row["driver"] = drv
                    producer.produce(topic=topic_position.name, key=str(drv), value=row)
                    count += 1
                logger.info(f"  Produced {len(pos)} position rows for driver {drv}")
    except Exception as e:
        logger.warning(f"  Position data error: {e}")

    logger.info(f"Session {event_name} {sess_name}: {count} total messages produced")
    return count > 0

def _serialize_row(row_dict):
    result = {}
    for k, v in row_dict.items():
        try:
            if hasattr(v, 'isoformat'):
                result[k] = v.isoformat()
            elif hasattr(v, 'item'):
                result[k] = float(v)
            elif v != v:  # NaN check
                pass
            else:
                result[k] = v
        except Exception:
            pass
    return result

if __name__ == "__main__":
    main()
