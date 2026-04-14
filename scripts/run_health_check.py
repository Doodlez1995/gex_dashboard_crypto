from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import OPTIONS_FILE, ALERT_WEBHOOK_URL
from pro.monitoring import health_report, send_webhook_alert


def main():
    if not Path(OPTIONS_FILE).exists():
        payload = {"ok": False, "issues": ["options file missing"]}
        print(payload)
        if ALERT_WEBHOOK_URL:
            send_webhook_alert(ALERT_WEBHOOK_URL, payload)
        return

    df = pd.read_csv(OPTIONS_FILE)
    stale_minutes = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(Path(OPTIONS_FILE).stat().st_mtime, unit="s", tz="UTC")).total_seconds() / 60.0
    report = health_report(df, stale_minutes=stale_minutes, min_rows=20, min_strikes=8)
    print(report)
    if (not report["ok"]) and ALERT_WEBHOOK_URL:
        send_webhook_alert(ALERT_WEBHOOK_URL, report)


if __name__ == "__main__":
    main()
