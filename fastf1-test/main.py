import logging
import os

import fastf1
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


def main():
    logger.info(
        f"Starting FastF1 connector (fastf1==3.8.3): year={YEAR}, round={ROUND}, session={SESSION_ID}"
    )

    app = Application()

    logger.info("Loading FastF1 session...")
    session = fastf1.get_session(2026, 1, "Q")
    session.load(
        laps=True, telemetry=True, weather=False, messages=False, livedata=None
    )
    print(session.drivers)


if __name__ == "__main__":
    main()
