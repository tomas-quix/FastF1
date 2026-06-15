import os
import sys
import time
import logging

import fastf1
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from quixstreams import Application

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def timedelta_to_ms(td):
    """Convert pandas Timedelta to integer milliseconds, or None if NaT."""
    if pd.isna(td):
        return None
    return int(td.total_seconds() * 1000)


def clean_value(val):
    """Convert numpy/pandas types to JSON-safe Python types."""
    if pd.isna(val):
        return None
    if hasattr(val, 'item'):  # numpy scalar
        return val.item()
    if isinstance(val, pd.Timedelta):
        return timedelta_to_ms(val)
    return val


def row_to_dict(row, columns):
    """Convert a DataFrame row to a clean dict with JSON-safe values."""
    return {col: clean_value(row[col]) for col in columns if col in row.index}


def produce_session_results(producer, topic, session, session_key):
    """Produce session results (final classification) to topic."""
    results = session.results
    if results is None or results.empty:
        logger.warning("No session results available")
        return 0

    count = 0
    columns = list(results.columns)
    for _, row in results.iterrows():
        msg = row_to_dict(row, columns)
        msg['_session_year'] = int(os.environ['YEAR'])
        msg['_session_gp'] = os.environ['GRAND_PRIX']
        msg['_session_type'] = os.environ['SESSION_TYPE']

        value = topic.serialize(key=session_key, value=msg)
        producer.produce(
            topic=topic.name,
            key=value.key,
            value=value.value,
        )
        count += 1
    logger.info(f"Produced {count} session result messages")
    return count


def produce_laps(producer, topic, session, session_key):
    """Produce lap-by-lap data to topic. Key = driver abbreviation."""
    laps = session.laps
    if laps is None or laps.empty:
        logger.warning("No lap data available")
        return 0

    count = 0
    columns = list(laps.columns)
    for _, row in laps.iterrows():
        msg = row_to_dict(row, columns)
        msg['_session_year'] = int(os.environ['YEAR'])
        msg['_session_gp'] = os.environ['GRAND_PRIX']
        msg['_session_type'] = os.environ['SESSION_TYPE']

        driver_key = str(row.get('Driver', session_key))
        value = topic.serialize(key=driver_key, value=msg)
        producer.produce(
            topic=topic.name,
            key=value.key,
            value=value.value,
        )
        count += 1
    logger.info(f"Produced {count} lap messages")
    return count


def produce_telemetry(producer, topic, session, session_key):
    """Produce high-frequency telemetry for all drivers. Key = driver abbreviation."""
    laps = session.laps
    if laps is None or laps.empty:
        logger.warning("No lap data available for telemetry")
        return 0

    drivers = laps['Driver'].unique()
    total_count = 0

    for driver in drivers:
        driver_laps = laps.pick_driver(driver)
        try:
            telemetry = driver_laps.get_telemetry()
        except Exception as e:
            logger.warning(f"Could not load telemetry for {driver}: {e}")
            continue

        if telemetry is None or telemetry.empty:
            continue

        columns = list(telemetry.columns)
        count = 0
        for _, row in telemetry.iterrows():
            msg = row_to_dict(row, columns)
            msg['Driver'] = str(driver)
            msg['_session_year'] = int(os.environ['YEAR'])
            msg['_session_gp'] = os.environ['GRAND_PRIX']
            msg['_session_type'] = os.environ['SESSION_TYPE']

            value = topic.serialize(key=str(driver), value=msg)
            producer.produce(
                topic=topic.name,
                key=value.key,
                value=value.value,
            )
            count += 1

        total_count += count
        logger.info(f"Produced {count} telemetry messages for {driver}")

    logger.info(f"Produced {total_count} total telemetry messages")
    return total_count


def produce_weather(producer, topic, session, session_key):
    """Produce weather data to topic."""
    weather = session.weather_data
    if weather is None or weather.empty:
        logger.warning("No weather data available")
        return 0

    count = 0
    columns = list(weather.columns)
    for _, row in weather.iterrows():
        msg = row_to_dict(row, columns)
        msg['_session_year'] = int(os.environ['YEAR'])
        msg['_session_gp'] = os.environ['GRAND_PRIX']
        msg['_session_type'] = os.environ['SESSION_TYPE']

        value = topic.serialize(key=session_key, value=msg)
        producer.produce(
            topic=topic.name,
            key=value.key,
            value=value.value,
        )
        count += 1
    logger.info(f"Produced {count} weather messages")
    return count


def main():
    year = int(os.environ['YEAR'])
    gp = os.environ['GRAND_PRIX']
    session_type = os.environ['SESSION_TYPE']

    # Setup FastF1 cache
    cache_dir = os.environ.get('Quix__Deployment__State__Path', '/tmp/fastf1_cache')
    os.makedirs(cache_dir, exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    logger.info(f"Loading FastF1 session: {year} {gp} {session_type}")
    session = fastf1.get_session(year, gp, session_type)

    max_retries = 3
    retry_delays = [10, 30, 60]

    for attempt in range(max_retries):
        try:
            logger.info(f"Loading session data (attempt {attempt + 1}/{max_retries})...")
            session.load()
            logger.info("Session loaded successfully")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delays[attempt]
                logger.warning(f"Failed to load session: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to load session after {max_retries} attempts: {e}")
                logger.error("The FastF1 API/mirror appears to be unavailable. Please try again later.")
                sys.exit(1)

    session_key = f"{year}-{gp}-{session_type}"

    # Create Quix Streams app and topics
    app = Application()

    topic_results = app.topic(os.environ['output_results'], value_serializer='json')
    topic_laps = app.topic(os.environ['output_laps'], value_serializer='json')
    topic_telemetry = app.topic(os.environ['output_telemetry'], value_serializer='json')
    topic_weather = app.topic(os.environ['output_weather'], value_serializer='json')

    with app.get_producer() as producer:
        results_count = produce_session_results(producer, topic_results, session, session_key)
        laps_count = produce_laps(producer, topic_laps, session, session_key)
        telemetry_count = produce_telemetry(producer, topic_telemetry, session, session_key)
        weather_count = produce_weather(producer, topic_weather, session, session_key)

        producer.flush()

    total = results_count + laps_count + telemetry_count + weather_count
    logger.info(f"Import complete! Total messages produced: {total}")

    # Summary of data layer availability
    layers = {
        'Results': results_count,
        'Laps': laps_count,
        'Telemetry': telemetry_count,
        'Weather': weather_count,
    }
    produced = [name for name, count in layers.items() if count > 0]
    empty = [name for name, count in layers.items() if count == 0]

    if produced:
        logger.info(f"Successfully produced: {', '.join(produced)}")
    if empty:
        logger.warning(f"Empty/unavailable: {', '.join(empty)}")

    logger.info(f"  Results: {results_count}, Laps: {laps_count}, Telemetry: {telemetry_count}, Weather: {weather_count}")


if __name__ == '__main__':
    main()
