"""
Lap join logic: maintains an in-memory sorted index of lap start times
and looks up the current lap number for a given car data timestamp.
"""
import bisect
from datetime import datetime, timezone


# In-memory lap index: {(session_key, driver_number): [(date_start_ts, lap_number), ...]}
# Each list is kept sorted by date_start_ts.
lap_index = {}


def parse_dt(s):
    """Parse an ISO datetime string to a UTC timestamp (float seconds)."""
    if s is None:
        return None
    # Normalise timezone suffix
    s_norm = s.replace("+00:00", "").replace("Z", "")
    # Strip trailing zeros after decimal point so fromisoformat is happy on 3.10-
    if "." in s_norm:
        s_norm = s_norm.rstrip("0").rstrip(".")
    try:
        dt = datetime.fromisoformat(s_norm)
    except ValueError:
        dt = datetime.fromisoformat(s_norm.split(".")[0])
    return dt.replace(tzinfo=timezone.utc).timestamp()


def process_lap(msg):
    """Index a lap record into the global lap_index."""
    session_key = msg.get("session_key")
    driver_number = msg.get("driver_number")
    lap_number = msg.get("lap_number")
    date_start = msg.get("date_start")
    if session_key is None or driver_number is None or lap_number is None or date_start is None:
        return
    ts = parse_dt(date_start)
    if ts is None:
        return
    index_key = (session_key, driver_number)
    entries = lap_index.setdefault(index_key, [])
    timestamps = [e[0] for e in entries]
    pos = bisect.bisect_left(timestamps, ts)
    entries.insert(pos, (ts, lap_number))


def get_lap_number(session_key, driver_number, date_str):
    """
    Return the lap_number active at *date_str* for the given driver/session,
    or None if no lap data is available.
    """
    key = (session_key, driver_number)
    entries = lap_index.get(key)
    if not entries:
        return None
    ts = parse_dt(date_str)
    if ts is None:
        return None
    timestamps = [e[0] for e in entries]
    idx = bisect.bisect_right(timestamps, ts) - 1
    if idx < 0:
        # Timestamp is before the first recorded lap start; attribute to the first lap.
        return entries[0][1]
    return entries[idx][1]
