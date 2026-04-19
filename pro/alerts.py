from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Dict, List, Optional

import pandas as pd

DEFAULT_RULES = [
    {"id": "flip_cross", "type": "flip_cross", "enabled": True, "severity": "medium"},
    {
        "id": "net_gex_threshold",
        "type": "net_gex_threshold",
        "enabled": True,
        "severity": "high",
        "threshold": 10_000_000_000,
        "direction": "abs",
    },
    {"id": "oi_wall_shift", "type": "oi_wall_shift", "enabled": True, "severity": "medium", "min_shift_pct": 0.03},
    {"id": "vol_regime_change", "type": "vol_regime_change", "enabled": True, "severity": "low"},
]


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_rules(path: Optional[Path]) -> List[Dict[str, Any]]:
    if not path:
        return DEFAULT_RULES
    try:
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
        rules = payload.get("rules") if isinstance(payload, dict) else None
        if isinstance(rules, list) and rules:
            return rules
    except OSError as exc:
        print(f"[alerts] cannot read rules {path}: {exc}; using defaults")
    except json.JSONDecodeError as exc:
        print(f"[alerts] malformed rules JSON {path}: {exc}; using defaults")
    return DEFAULT_RULES


def _flip_cross_alert(context: Dict[str, Any], rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prev_flip = _safe_float(context.get("prev_flip"))
    curr_flip = _safe_float(context.get("curr_flip"))
    spot = _safe_float(context.get("spot"))
    if prev_flip is None or curr_flip is None or spot is None:
        return None
    prev_side = "above" if spot > prev_flip else "below"
    curr_side = "above" if spot > curr_flip else "below"
    if prev_side == curr_side:
        return None
    return {
        "rule_id": rule.get("id", "flip_cross"),
        "severity": rule.get("severity", "medium"),
        "message": f"Spot crossed flip: {prev_side} -> {curr_side} (prev {prev_flip:.0f}, curr {curr_flip:.0f})",
        "payload": {"prev_flip": prev_flip, "curr_flip": curr_flip, "spot": spot},
    }


def _net_gex_threshold_alert(context: Dict[str, Any], rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    net_gex = _safe_float(context.get("net_gex"))
    threshold = _safe_float(rule.get("threshold"))
    direction = str(rule.get("direction", "abs")).lower()
    if net_gex is None or threshold is None:
        return None
    triggered = False
    if direction == "abs":
        triggered = abs(net_gex) >= threshold
    elif direction == "above":
        triggered = net_gex >= threshold
    elif direction == "below":
        triggered = net_gex <= -abs(threshold)
    if not triggered:
        return None
    return {
        "rule_id": rule.get("id", "net_gex_threshold"),
        "severity": rule.get("severity", "high"),
        "message": f"Net GEX threshold hit: {net_gex:,.0f}",
        "payload": {"net_gex": net_gex, "threshold": threshold, "direction": direction},
    }


def _oi_wall_shift_alert(context: Dict[str, Any], rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prev_wall = _safe_float(context.get("prev_oi_wall"))
    curr_wall = _safe_float(context.get("oi_wall"))
    spot = _safe_float(context.get("spot"))
    if prev_wall is None or curr_wall is None:
        return None
    shift_abs = abs(curr_wall - prev_wall)
    min_shift_abs = _safe_float(rule.get("min_shift_abs"))
    min_shift_pct = _safe_float(rule.get("min_shift_pct"))
    pct = None
    if spot and spot > 0:
        pct = shift_abs / spot
    triggered = False
    if min_shift_abs is not None and shift_abs >= min_shift_abs:
        triggered = True
    if min_shift_pct is not None and pct is not None and pct >= min_shift_pct:
        triggered = True
    if not triggered:
        return None
    pct_text = f" ({pct * 100:.2f}%)" if pct is not None else ""
    return {
        "rule_id": rule.get("id", "oi_wall_shift"),
        "severity": rule.get("severity", "medium"),
        "message": f"OI wall shifted {shift_abs:.0f}{pct_text}: {prev_wall:.0f} -> {curr_wall:.0f}",
        "payload": {"prev_oi_wall": prev_wall, "oi_wall": curr_wall, "spot": spot},
    }


def _vol_regime_change_alert(context: Dict[str, Any], rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prev_regime = str(context.get("prev_vol_regime") or "").lower()
    curr_regime = str(context.get("vol_regime") or "").lower()
    if not prev_regime or not curr_regime:
        return None
    if prev_regime == curr_regime:
        return None
    if "unknown" in (prev_regime, curr_regime):
        return None
    return {
        "rule_id": rule.get("id", "vol_regime_change"),
        "severity": rule.get("severity", "low"),
        "message": f"Vol regime changed: {prev_regime} -> {curr_regime}",
        "payload": {"prev_vol_regime": prev_regime, "vol_regime": curr_regime},
    }


def evaluate_rules(
    context: Dict[str, Any],
    rules_path: Optional[Path] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    selected_rules = rules if rules is not None else load_rules(rules_path)
    alerts: List[Dict[str, Any]] = []
    for rule in selected_rules:
        if not rule or not rule.get("enabled", True):
            continue
        rule_type = str(rule.get("type", "")).lower()
        alert = None
        if rule_type == "flip_cross":
            alert = _flip_cross_alert(context, rule)
        elif rule_type == "net_gex_threshold":
            alert = _net_gex_threshold_alert(context, rule)
        elif rule_type == "oi_wall_shift":
            alert = _oi_wall_shift_alert(context, rule)
        elif rule_type == "vol_regime_change":
            alert = _vol_regime_change_alert(context, rule)
        if alert:
            alert["ts_utc"] = context.get("ts_utc") or pd.Timestamp.now(tz="UTC").isoformat()
            alert["symbol"] = context.get("symbol") or "unknown"
            alerts.append(alert)
    return alerts
