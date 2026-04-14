from typing import Dict, Optional, Any

import pandas as pd
import requests

from pro.alerts import evaluate_rules


def health_report(df: pd.DataFrame, stale_minutes: Optional[float], min_rows: int, min_strikes: int) -> Dict:
    issues = []
    if stale_minutes is not None and stale_minutes > 20:
        issues.append(f"stale>{stale_minutes:.1f}m")
    if df is None or df.empty:
        issues.append("empty")
        return {"ok": False, "issues": issues}
    if len(df) < min_rows:
        issues.append(f"rows<{min_rows}")
    if int(df["strike"].nunique()) < min_strikes:
        issues.append(f"strikes<{min_strikes}")
    return {"ok": len(issues) == 0, "issues": issues}


def evaluate_alerts(
    prev_flip: Optional[float],
    curr_flip: Optional[float],
    spot: float,
    context: Optional[Dict[str, Any]] = None,
    rules_path=None,
) -> Dict[str, Any]:
    ctx = {
        "prev_flip": prev_flip,
        "curr_flip": curr_flip,
        "spot": spot,
    }
    if context:
        ctx.update(context)
    return {"alerts": evaluate_rules(ctx, rules_path=rules_path), "context": ctx}


def send_webhook_alert(webhook_url: str, payload: Dict) -> bool:
    if not webhook_url:
        return False
    try:
        r = requests.post(webhook_url, json=payload, timeout=5)
        return r.status_code >= 200 and r.status_code < 300
    except requests.RequestException:
        return False
