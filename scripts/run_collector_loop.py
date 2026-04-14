import argparse
import json
import time
from pathlib import Path

import pandas as pd

from collector import run_collection
from config import DATA_DIR

STATUS_FILE = DATA_DIR / "collector_status.json"


def write_status(ok: bool, rows: int = 0, error: str = "") -> None:
    payload = {
        "ts_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "ok": bool(ok),
        "rows": int(rows),
        "error": error,
    }
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the collector on a fixed cadence.")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between collection runs.")
    parser.add_argument("--once", action="store_true", help="Run a single collection cycle and exit.")
    args = parser.parse_args()

    while True:
        try:
            df = run_collection()
            write_status(True, rows=len(df))
        except Exception as exc:
            write_status(False, rows=0, error=str(exc))
        if args.once:
            break
        time.sleep(max(5, int(args.interval)))


if __name__ == "__main__":
    main()
