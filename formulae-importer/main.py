import os
import re
import time
import logging

from dotenv import load_dotenv
load_dotenv()

import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "QuixFormulaEDemo/1.0 (contact: demo@quix.io)"

# Race classification tables have these columns; qualifying tables share the
# leading columns but have no Laps/Time-Retired/Points.
RACE_REQUIRED_COLUMNS = {"Pos.", "No.", "Driver", "Team", "Laps", "Time/Retired", "Grid"}
QUALIFYING_REQUIRED_COLUMNS = {"Pos.", "No.", "Driver", "Team", "Grid"}

# Best-effort country name -> IOC-style 3-letter code lookup for the flag
# icons used in Wikipedia's Formula E classification tables. Not exhaustive —
# unmapped countries fall back to an empty string per the flat schema spec.
COUNTRY_CODES = {
    "Argentina": "ARG", "Australia": "AUS", "Austria": "AUT", "Barbados": "BAR",
    "Belgium": "BEL", "Brazil": "BRA", "Canada": "CAN", "Chile": "CHI",
    "China": "CHN", "Colombia": "COL", "Denmark": "DEN", "Estonia": "EST",
    "Finland": "FIN", "France": "FRA", "Germany": "GER", "India": "IND",
    "Italy": "ITA", "Japan": "JPN", "Mexico": "MEX", "Monaco": "MON",
    "Netherlands": "NED", "New Zealand": "NZL", "Poland": "POL",
    "Portugal": "POR", "Russia": "RUS", "South Africa": "RSA", "Spain": "ESP",
    "Sweden": "SWE", "Switzerland": "SUI", "Thailand": "THA",
    "United Kingdom": "GBR", "United States": "USA",
}

FOOTNOTE_RE = re.compile(r"\[[^\]]*\]")


def _strip_footnote(value) -> str:
    """Strip trailing wiki footnote markers like '[g]' and surrounding whitespace."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return FOOTNOTE_RE.sub("", text).strip()


def _to_int(value, default: int = 0) -> int:
    """Extract the leading run of digits from a string and cast to int."""
    text = _strip_footnote(value)
    match = re.search(r"\d+", text)
    return int(match.group()) if match else default


def _to_points(value) -> float:
    """Extract base points, dropping bonus-point/footnote suffixes.

    e.g. "18+1<footnote>" (rendered by pandas as "18+11") -> 18.0
    """
    text = _strip_footnote(value)
    match = re.match(r"^(\d+)", text)
    return float(match.group(1)) if match else 0.0


def _is_data_row(row: dict) -> bool:
    """Filter out footer/source rows (e.g. 'Source: [7]') that read_html
    picks up as extra table rows — a real row always has a numeric car number.
    """
    return bool(re.search(r"\d", _strip_footnote(row.get("No."))))


def _table_columns(df: pd.DataFrame) -> set:
    return {str(c) for c in df.columns}


def _classify_table(df: pd.DataFrame):
    """Return 'Race', 'Qualifying', or None for a parsed wikitable."""
    cols = _table_columns(df)
    if RACE_REQUIRED_COLUMNS.issubset(cols):
        return "Race"
    if QUALIFYING_REQUIRED_COLUMNS.issubset(cols):
        return "Qualifying"
    return None


def _row_nationality(html_table, data_row_index: int) -> str:
    """Look up the flag alt text for a data row via the raw HTML table,
    aligned by position (row 0 in html_table is the header row).
    """
    trs = html_table.find_all("tr")
    row_pos = data_row_index + 1  # skip header row
    if row_pos >= len(trs):
        return ""
    img = trs[row_pos].find("img")
    if img and img.get("alt"):
        return COUNTRY_CODES.get(img["alt"], "")
    return ""


def parse_html_to_rows(html: str, event_name: str, season: str) -> list[dict]:
    """Parse a Wikipedia event page's HTML into flattened classification rows.

    Pure function — no network or Kafka calls — so it can be unit tested
    against a saved HTML fixture.
    """
    tables = pd.read_html(StringIO(html))
    soup = BeautifulSoup(html, "lxml")
    html_tables = soup.find_all("table")

    rows: list[dict] = []
    race_counter = 0
    qualifying_counter = 0

    for table_index, df in enumerate(tables):
        session_type = _classify_table(df)
        if session_type is None:
            continue

        if session_type == "Race":
            race_counter += 1
            race_number = race_counter
        else:
            qualifying_counter += 1
            race_number = qualifying_counter

        html_table = html_tables[table_index] if table_index < len(html_tables) else None
        df = df.reset_index(drop=True)
        n_rows = 0

        for data_row_index, record in enumerate(df.to_dict(orient="records")):
            if not _is_data_row(record):
                continue

            nationality = ""
            if html_table is not None:
                nationality = _row_nationality(html_table, data_row_index)

            rows.append({
                "season": season,
                "event_name": event_name,
                "race_number": race_number,
                "session_type": session_type,
                "position": _to_int(record.get("Pos."), default=data_row_index + 1),
                "car_number": _to_int(record.get("No.")),
                "driver_name": _strip_footnote(record.get("Driver")),
                "driver_nationality": nationality,
                "team": _strip_footnote(record.get("Team")),
                "laps": _to_int(record.get("Laps")),
                "time_or_gap": _strip_footnote(record.get("Time/Retired")),
                "grid": _to_int(record.get("Grid")),
                "points": _to_points(record.get("Points")),
                "ts_ms": int(time.time() * 1000),
            })
            n_rows += 1

        logger.info(f"  Parsed {n_rows} row(s) for {event_name} — {session_type} #{race_number}")

    return rows


def fetch_page_html(page_title: str) -> str:
    """Fetch a Wikipedia page's rendered HTML via the Action API."""
    resp = requests.get(
        WIKI_API_URL,
        params={"action": "parse", "page": page_title, "prop": "text", "format": "json"},
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["parse"]["text"]["*"]


def fetch_and_parse(page_title: str, season: str) -> list[dict]:
    """Fetch a Formula E event page from Wikipedia and return flattened rows."""
    logger.info(f"Fetching {page_title} ...")
    html = fetch_page_html(page_title)
    event_name = page_title.replace("_", " ")
    rows = parse_html_to_rows(html, event_name, season)
    logger.info(f"  → {len(rows)} total row(s) parsed for {event_name}")
    return rows


def main():
    output_topic_name = os.environ["output"]
    event_pages = [p.strip() for p in os.environ.get("FE_EVENT_PAGES", "2025_Berlin_ePrix").split(",") if p.strip()]
    season = os.environ.get("FE_SEASON", "2024-25")

    app = Application()
    topic = app.topic(output_topic_name, value_serializer="json")

    total = 0
    with app.get_producer() as producer:
        for page_title in event_pages:
            rows = fetch_and_parse(page_title, season)
            for row in rows:
                msg_key = f"{row['event_name']}-{row['session_type']}-{row['race_number']}-{row['car_number']}"
                msg = topic.serialize(key=msg_key, value=row)
                producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            total += len(rows)

    logger.info(f"=== Import complete: {total} row(s) published to {output_topic_name} ===")


if __name__ == "__main__":
    main()
