from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (
    ACCOUNT_EQUITY_USD,
    OPTIONS_FILE,
    TELEGRAM_ALLOWED_CHAT_IDS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHANNEL_ID,
    TELEGRAM_DEFAULT_DTE_DAYS,
    TELEGRAM_DEFAULT_EXCHANGES,
    TELEGRAM_DEFAULT_SYMBOL,
)
from pro.strategies import generate_professional_ideas
from app import (
    DERIBIT_LOOKBACK_BY_RESOLUTION,
    DERIBIT_RESOLUTION,
    DEFAULT_SESSION_BARS,
    build_option_heatmap_tool,
    build_spot_figure,
    fetch_deribit_candles,
    resolution_to_minutes,
)

API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
HELP_TEXT = (
    "Commands:\n"
    "/levels [BTC|ETH] [weekly|monthly|multi4|all] [broadcast]\n"
    "Examples:\n"
    "/levels BTC weekly\n"
    "/levels ETH multi4\n"
    "\n"
    "Notes:\n"
    "- broadcast posts to TELEGRAM_CHANNEL_ID if configured.\n"
    "- Data is sourced from options_data.csv.\n"
    "- Bot replies include levels text + GEX/spot charts.\n"
    "- Use /start (or type menu) to show buttons.\n"
    "- Tap 💡 Trading Ideas after levels to request setups.\n"
    "- You can also set a custom From/To range from available expiries.\n"
    "- If no range is provided, weekly is used."
)

INFO_TEXT = (
    "ℹ️ Trading read of levels\n"
    "• 💵 SP: current spot price.\n"
    "• 🎯 MP (Max Pain): strike where net GEX is smallest (pin risk zone).\n"
    "• 🧭 GF (Gamma Flip): where net GEX changes sign (regime pivot).\n"
    "• 🟢 P1/P2: strongest +GEX strikes (often act as support / stabilization).\n"
    "• 🔴 N1/N2: strongest -GEX strikes (often act as resistance / acceleration).\n"
    "• 🟠 A1/A2: largest absolute positioning (high-attention magnet levels).\n"
    "\n"
    "Trading notes:\n"
    "• Above GF = dealer long gamma bias; below GF = more directional risk and faster moves.\n"
    "• Expect pinning into MP/A-levels near expiry; watch breaks for momentum.\n"
    "• Weekly vs Monthly: weekly is near‑term flow, monthly is cycle positioning.\n"
)

NET_GEX_SCALE = 1_000.0
AG_SCALE = 1_000_000.0
GEX_IMAGE_SIZE = (1200, 650)
SPOT_IMAGE_SIZE = (1200, 700)
HEATMAP_IMAGE_SIZE = (1200, 600)
EXPIRY_PAGE_SIZE = 9

LAST_REQUESTS: Dict[int, Dict[str, object]] = {}
RANGE_STATE: Dict[int, Dict[str, Optional[str]]] = {}


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_price(value: Optional[float]) -> str:
    if value is None or not pd.notna(value):
        return "n/a"
    return f"{float(value):,.0f}"


def fmt_metric(value: Optional[float]) -> str:
    if value is None or not pd.notna(value):
        return "n/a"
    abs_v = abs(float(value))
    if abs_v >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if abs_v >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_v >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:.0f}"


def fmt_money(value: Optional[float]) -> str:
    if value is None or not pd.notna(value):
        return "n/a"
    abs_v = abs(float(value))
    if abs_v >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_v >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"


def fmt_pct(value: Optional[float]) -> str:
    if value is None or not pd.notna(value):
        return "n/a"
    return f"{float(value):.2f}%"


def pct_delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    if not pd.notna(a) or not pd.notna(b):
        return None
    base = float(a)
    if base == 0:
        return None
    return (float(b) - base) / base * 100.0


def canonical_exchange_name(raw_value: object) -> Optional[str]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    lower = text.lower()
    if lower == "deribit":
        return "Deribit"
    if lower == "bybit":
        return "Bybit"
    if lower == "binance":
        return "Binance"
    if lower in {"okx", "okx.com"}:
        return "OKX"
    return text


def ordered_exchange_list(values: Sequence[object]) -> List[str]:
    order = ["Deribit", "Bybit", "Binance", "OKX"]
    seen: List[str] = []
    for item in values:
        normalized = canonical_exchange_name(item)
        if normalized and normalized not in seen:
            seen.append(normalized)
    priority = {name: idx for idx, name in enumerate(order)}
    return sorted(seen, key=lambda name: (priority.get(name, len(priority)), name))


def gamma_flip_level(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty:
        return None
    df_sorted = df.sort_values("strike")[["strike", "total_gex"]].copy()
    df_sorted["strike"] = pd.to_numeric(df_sorted["strike"], errors="coerce")
    df_sorted["total_gex"] = pd.to_numeric(df_sorted["total_gex"], errors="coerce").fillna(0.0)
    df_sorted = df_sorted.dropna(subset=["strike"])
    if df_sorted.empty:
        return None

    strikes = df_sorted["strike"].astype(float).tolist()
    gex_values = df_sorted["total_gex"].astype(float).tolist()
    closest_strike = strikes[min(range(len(gex_values)), key=lambda i: abs(gex_values[i]))]

    for i in range(len(gex_values) - 1):
        g1 = gex_values[i]
        g2 = gex_values[i + 1]
        s1 = strikes[i]
        s2 = strikes[i + 1]
        if g1 == 0:
            return s1
        if g2 == 0:
            return s2
        if (g1 < 0 < g2) or (g1 > 0 > g2):
            weight = -g1 / (g2 - g1)
            return s1 + weight * (s2 - s1)

    return closest_strike


def parse_allowed_chat_ids(raw_value: str) -> set:
    if not raw_value:
        return set()
    out = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            out.add(int(item))
        except ValueError:
            out.add(item)
    return out


def is_allowed_chat(chat_id: Optional[int], allowed: set) -> bool:
    if chat_id is None:
        return False
    if not allowed:
        return True
    if chat_id in allowed or str(chat_id) in allowed:
        return True
    return False


def parse_exchange_list(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    out = []
    for item in raw_value.split(","):
        normalized = canonical_exchange_name(item)
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def parse_levels_args(args: Sequence[str]) -> Tuple[str, Optional[int], bool, Optional[str], Optional[int]]:
    symbol = (TELEGRAM_DEFAULT_SYMBOL or "BTC").upper()
    days = TELEGRAM_DEFAULT_DTE_DAYS
    use_all = False
    mode: Optional[str] = None
    multi_count: Optional[int] = None
    explicit_range = False
    for raw in args:
        token = str(raw).strip().lower()
        if not token:
            continue
        if token in {"btc", "eth"}:
            symbol = token.upper()
            continue
        if token in {"weekly", "week", "w"}:
            mode = "weekly"
            days = None
            explicit_range = True
            continue
        if token in {"monthly", "month", "m"}:
            mode = "monthly"
            days = None
            explicit_range = True
            continue
        if token.startswith("multi") and token[5:].isdigit():
            mode = "multi"
            multi_count = int(token[5:])
            days = None
            explicit_range = True
            continue
        if token.endswith("w") and token[:-1].isdigit():
            mode = "multi"
            multi_count = int(token[:-1])
            days = None
            explicit_range = True
            continue
        if token in {"all", "*"}:
            use_all = True
            days = None
            explicit_range = True
            continue
        if token.endswith("d") and token[:-1].isdigit():
            days = int(token[:-1])
            explicit_range = True
            continue
        if token.isdigit():
            days = int(token)
            explicit_range = True
            continue
    if use_all:
        days = None
    if days is not None and days <= 0:
        days = None
    if not explicit_range:
        mode = "weekly"
        days = None
    return symbol, days, use_all, mode, multi_count


def parse_levels_command(text: str) -> Optional[List[str]]:
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    if stripped.startswith("/"):
        parts = stripped.split()
        command = parts[0].split("@")[0].lower()
        if command in {"/levels", "/level", "/gex"}:
            return parts[1:]
        if command in {"/start"}:
            return ["__menu__"]
        if command in {"/help"}:
            return ["__help__"]
        return None
    if stripped.lower() in {"menu", "start", "help"}:
        return ["__menu__"]
    if stripped.lower().startswith("levels"):
        return stripped.split()[1:]
    return None


def load_options_frame() -> pd.DataFrame:
    options_path = Path(OPTIONS_FILE)
    if not options_path.exists():
        raise FileNotFoundError(f"Options file missing: {options_path}")
    df = pd.read_csv(options_path)
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.tz_localize(None)
    if "exchange" not in df.columns:
        df["exchange"] = "Deribit"
    df["exchange"] = df["exchange"].map(canonical_exchange_name).fillna(df["exchange"])
    return df


def options_data_timestamp() -> Tuple[str, Optional[float]]:
    try:
        ts = Path(OPTIONS_FILE).stat().st_mtime
    except OSError:
        return "n/a", None
    dt = pd.Timestamp(ts, unit="s", tz="UTC")
    age_minutes = (pd.Timestamp.now(tz="UTC") - dt).total_seconds() / 60.0
    return dt.strftime("%Y-%m-%d %H:%M UTC"), age_minutes


def is_last_friday(date_val: pd.Timestamp) -> bool:
    if date_val.weekday() != 4:
        return False
    return (date_val + pd.Timedelta(days=7)).month != date_val.month


def monthly_expiries(expiries: List[pd.Timestamp]) -> List[pd.Timestamp]:
    if not expiries:
        return []
    grouped: Dict[Tuple[int, int], List[pd.Timestamp]] = {}
    for item in expiries:
        key = (int(item.year), int(item.month))
        grouped.setdefault(key, []).append(item)
    monthly_list: List[pd.Timestamp] = []
    for _key, month_items in grouped.items():
        month_items = sorted(month_items)
        last_fridays = [d for d in month_items if is_last_friday(d)]
        if last_fridays:
            monthly_list.append(last_fridays[-1])
        else:
            monthly_list.append(month_items[-1])
    return sorted(monthly_list)


def select_expiry_window(
    dff: pd.DataFrame,
    days: Optional[int],
    mode: Optional[str],
    multi_count: Optional[int],
) -> Tuple[pd.DataFrame, Optional[str], str]:
    note = None
    today = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    forward = dff[dff["expiry"] >= today].copy()
    expiries = sorted({pd.Timestamp(x).normalize() for x in forward["expiry"].dropna().tolist()})

    if mode == "monthly":
        monthly = monthly_expiries(expiries)
        if monthly:
            pick = monthly[0]
            filtered = dff[dff["expiry"] == pick]
            return filtered, None, "next monthly expiry"
        note = "No monthly expiry found; using all expiries."
        return dff, note, "all expiries"

    if mode == "weekly":
        monthly = set(monthly_expiries(expiries))
        weekly = [d for d in expiries if d.weekday() == 4 and d not in monthly]
        if not weekly:
            weekly = [d for d in expiries if d.weekday() == 4]
        if weekly:
            pick = weekly[0]
            filtered = dff[dff["expiry"] == pick]
            return filtered, None, "next weekly expiry"
        note = "No weekly expiry found; using all expiries."
        return dff, note, "all expiries"

    if mode == "multi":
        count = max(int(multi_count or 4), 1)
        monthly = set(monthly_expiries(expiries))
        weekly = [d for d in expiries if d.weekday() == 4 and d not in monthly]
        if not weekly:
            weekly = [d for d in expiries if d.weekday() == 4]
        if weekly:
            picks = weekly[:count]
            filtered = dff[dff["expiry"].isin(picks)]
            return filtered, None, f"next {len(picks)} weekly expiries"
        note = "No weekly expiries found; using all expiries."
        return dff, note, "all expiries"

    if days is not None:
        end = today + pd.Timedelta(days=int(days))
        window = dff[(dff["expiry"] >= today) & (dff["expiry"] <= end)]
        if not window.empty:
            return window, None, f"next {int(days)}d"
        note = f"No expiries in next {days}d; using all expiries."
        return dff, note, "all expiries"

    return dff, None, "all expiries"


def compute_levels(
    df: pd.DataFrame,
    symbol: str,
    exchanges: Sequence[str],
    days: Optional[int],
    mode: Optional[str],
    multi_count: Optional[int],
    start_expiry: Optional[str] = None,
    end_expiry: Optional[str] = None,
) -> Tuple[Optional[Dict], Optional[str], Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
    note = None
    window_label = "all expiries"
    if df is None or df.empty:
        return None, "No options data available.", None, None, window_label

    dff = df[df["symbol"] == symbol]
    if exchanges:
        dff = dff[dff["exchange"].isin(exchanges)]
    if dff.empty:
        return None, "No data available for the requested symbol/exchanges.", None, None, window_label

    if start_expiry and end_expiry:
        start = pd.to_datetime(start_expiry).normalize()
        end = pd.to_datetime(end_expiry).normalize()
        dff = dff[(dff["expiry"] >= start) & (dff["expiry"] <= end)]
        window_label = f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
        if dff.empty:
            return None, "No data available for the selected expiry range.", None, None, window_label
    else:
        dff, note, window_label = select_expiry_window(dff, days=days, mode=mode, multi_count=multi_count)

    dff = dff.copy()
    dff["strike"] = pd.to_numeric(dff["strike"], errors="coerce")
    dff = dff.dropna(subset=["strike"])
    if dff.empty:
        return None, "No valid strikes available for the request.", None, None, window_label

    dff = dff.assign(
        total_gex=dff["call_gex"] + dff["put_gex"],
        abs_gex=dff["call_gex"].abs() + dff["put_gex"].abs(),
    )

    gex_by_strike = dff.groupby("strike", as_index=False)[["total_gex", "abs_gex"]].sum()
    if gex_by_strike.empty:
        return None, "No GEX data available after aggregation.", None, None, window_label

    spot_value = None
    if "spot_price" in dff.columns:
        spot_series = pd.to_numeric(dff["spot_price"], errors="coerce").dropna()
        if not spot_series.empty:
            spot_value = float(spot_series.iloc[-1])

    mp_strike = float(gex_by_strike.loc[gex_by_strike["total_gex"].abs().idxmin(), "strike"])
    flip_value = gamma_flip_level(gex_by_strike)
    flip_strike = float(flip_value) if flip_value is not None and pd.notna(flip_value) else None

    pos = gex_by_strike[gex_by_strike["total_gex"] > 0].nlargest(2, "total_gex")["strike"].tolist()
    neg = gex_by_strike[gex_by_strike["total_gex"] < 0].nsmallest(2, "total_gex")["strike"].tolist()
    abs_levels = gex_by_strike.nlargest(2, "abs_gex")["strike"].tolist()

    top_pos = None
    top_neg = None
    if (gex_by_strike["total_gex"] > 0).any():
        top_pos = gex_by_strike.loc[gex_by_strike["total_gex"].idxmax()]
    if (gex_by_strike["total_gex"] < 0).any():
        top_neg = gex_by_strike.loc[gex_by_strike["total_gex"].idxmin()]
    top_abs = gex_by_strike.loc[gex_by_strike["abs_gex"].idxmax()]

    expiry_min = dff["expiry"].min()
    expiry_max = dff["expiry"].max()

    return (
        {
            "symbol": symbol,
            "spot": spot_value,
            "mp": mp_strike,
            "flip": flip_strike,
            "p1": pos[0] if len(pos) > 0 else None,
            "p2": pos[1] if len(pos) > 1 else None,
            "n1": neg[0] if len(neg) > 0 else None,
            "n2": neg[1] if len(neg) > 1 else None,
            "a1": abs_levels[0] if len(abs_levels) > 0 else None,
            "a2": abs_levels[1] if len(abs_levels) > 1 else None,
            "net_total": float(gex_by_strike["total_gex"].sum()),
            "expiry_min": expiry_min,
            "expiry_max": expiry_max,
            "expiry_count": int(dff["expiry"].nunique()),
            "exchanges": ordered_exchange_list(dff["exchange"].dropna().tolist()),
            "row_count": int(len(dff)),
            "strike_count": int(dff["strike"].nunique()),
            "top_pos_strike": float(top_pos["strike"]) if top_pos is not None else None,
            "top_pos_value": float(top_pos["total_gex"]) if top_pos is not None else None,
            "top_neg_strike": float(top_neg["strike"]) if top_neg is not None else None,
            "top_neg_value": float(top_neg["total_gex"]) if top_neg is not None else None,
            "top_abs_strike": float(top_abs["strike"]) if top_abs is not None else None,
            "top_abs_value": float(top_abs["abs_gex"]) if top_abs is not None else None,
        },
        note,
        gex_by_strike,
        dff,
        window_label,
    )


def format_levels_message(levels: Dict, note: Optional[str], window_label: str) -> str:
    symbol = levels["symbol"]
    expiry_min = levels["expiry_min"]
    expiry_max = levels["expiry_max"]
    expiry_count = levels["expiry_count"]
    exchanges = ", ".join(levels["exchanges"]) if levels.get("exchanges") else "n/a"

    expiry_text = "n/a"
    if pd.notna(expiry_min) and pd.notna(expiry_max):
        expiry_text = f"{pd.Timestamp(expiry_min).strftime('%Y-%m-%d')} to {pd.Timestamp(expiry_max).strftime('%Y-%m-%d')}"

    data_ts, data_age = options_data_timestamp()
    age_text = f" | Age {data_age:.1f}m" if data_age is not None else ""

    spot = levels.get("spot")
    mp = levels.get("mp")
    flip = levels.get("flip")
    deltas = []
    if spot is not None and flip is not None and pd.notna(spot) and pd.notna(flip):
        side = "above" if float(spot) > float(flip) else "below"
        dist = abs(float(spot) - float(flip)) / float(spot) * 100.0 if float(spot) != 0 else None
        if dist is not None:
            deltas.append(f"Spot {side} GF by {dist:.2f}%")
    if spot is not None and mp is not None and pd.notna(spot) and pd.notna(mp):
        side = "above" if float(spot) > float(mp) else "below"
        dist = abs(float(spot) - float(mp)) / float(spot) * 100.0 if float(spot) != 0 else None
        if dist is not None:
            deltas.append(f"Spot {side} MP by {dist:.2f}%")
    delta_text = f" ({'; '.join(deltas)})" if deltas else ""

    net_total = float(levels.get("net_total") or 0.0)
    if net_total > 0:
        net_bias = "positive"
    elif net_total < 0:
        net_bias = "negative"
    else:
        net_bias = "flat"

    peak_parts = []
    if levels.get("top_pos_strike") is not None:
        peak_parts.append(
            f"Top +GEX {fmt_price(levels.get('top_pos_strike'))} ({fmt_metric(levels.get('top_pos_value'))})"
        )
    if levels.get("top_neg_strike") is not None:
        peak_parts.append(
            f"Top -GEX {fmt_price(levels.get('top_neg_strike'))} ({fmt_metric(levels.get('top_neg_value'))})"
        )
    peaks_text = " | ".join(peak_parts)

    window_text = window_label or "all expiries"
    row_count = levels.get("row_count")
    strike_count = levels.get("strike_count")

    regime = "n/a"
    action_line = "🎯 Playbook n/a"
    p1 = levels.get("p1")
    p2 = levels.get("p2")
    n1 = levels.get("n1")
    n2 = levels.get("n2")
    a1 = levels.get("a1")
    a2 = levels.get("a2")

    if spot is not None and flip is not None and pd.notna(spot) and pd.notna(flip):
        above = float(spot) > float(flip)
        regime = "above GF (supportive flows)" if above else "below GF (trend risk)"
        if above:
            action_line = (
                "🎯 Playbook: Fade rips into "
                f"{fmt_price(n1)}/{fmt_price(n2)}; "
                f"buy dips near {fmt_price(p1)}/{fmt_price(p2)} "
                f"or {fmt_price(a1)}/{fmt_price(a2)}. "
                "Stop if GF breaks."
            )
        else:
            action_line = (
                "🎯 Playbook: Sell rips into "
                f"{fmt_price(n1)}/{fmt_price(n2)}; "
                f"cover near {fmt_price(p1)}/{fmt_price(p2)} "
                f"or {fmt_price(a1)}/{fmt_price(a2)}. "
                "Stop if GF reclaims."
            )
    bias_line = f"🧭 Regime {regime} | Net GEX {net_bias}"

    lines = [
        f"📊 GEX Levels — {symbol}",
        f"💵 Spot {fmt_price(spot)} | 🎯 MP {fmt_price(mp)} | 🧭 GF {fmt_price(flip)}{delta_text}",
        f"🟢 P1 {fmt_price(levels['p1'])} | P2 {fmt_price(levels['p2'])} | 🔴 N1 {fmt_price(levels['n1'])} | N2 {fmt_price(levels['n2'])}",
        f"🟠 A1 {fmt_price(levels['a1'])} | A2 {fmt_price(levels['a2'])}",
        bias_line + (f" | ⚡ {fmt_metric(net_total)}" if net_total is not None else ""),
        action_line,
        ("🏁 Key clusters: " + peaks_text) if peaks_text else "🏁 Key clusters: n/a",
        f"📅 Window {window_text} | Expiries {expiry_text} ({expiry_count})",
        f"🏷 Exchanges {exchanges} | 🧾 Rows {row_count} | Strikes {strike_count}",
        f"⏱ Data {data_ts}{age_text}",
    ]
    if note:
        lines.append(note)
    return "\n".join(lines)


def build_gex_figure(levels: Dict, gex_by_strike: pd.DataFrame) -> go.Figure:
    if gex_by_strike is None or gex_by_strike.empty:
        fig = go.Figure()
        fig.update_layout(
            paper_bgcolor="#05070a",
            plot_bgcolor="#0a0d10",
            font={"color": "#f6f0d5"},
            xaxis={"visible": False},
            yaxis={"visible": False},
            margin={"l": 10, "r": 10, "t": 10, "b": 10},
            annotations=[{"text": "GEX unavailable", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return fig

    df = gex_by_strike.copy()
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["total_gex"] = pd.to_numeric(df["total_gex"], errors="coerce").fillna(0.0)
    df["abs_gex"] = pd.to_numeric(df["abs_gex"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["strike"])
    if df.empty:
        return build_gex_figure(levels, pd.DataFrame())

    pos = df[df["total_gex"] >= 0].copy()
    neg = df[df["total_gex"] < 0].copy()

    fig = go.Figure()
    if not pos.empty:
        fig.add_bar(
            x=pos["strike"],
            y=pos["total_gex"] / NET_GEX_SCALE,
            name="Net GEX +",
            marker_color="#2dbd78",
            opacity=0.88,
        )
    if not neg.empty:
        fig.add_bar(
            x=neg["strike"],
            y=neg["total_gex"] / NET_GEX_SCALE,
            name="Net GEX -",
            marker_color="#ff5c5c",
            opacity=0.88,
        )

    fig.add_trace(
        go.Scatter(
            x=df["strike"],
            y=df["abs_gex"] / AG_SCALE,
            mode="lines",
            line={"color": "#ffcf88", "width": 2},
            name="AG (Abs GEX)",
            yaxis="y2",
        )
    )
    fig.add_hline(y=0, line_width=1, line_color="#4a5564")

    line_levels = [
        {"x": levels.get("spot"), "dash": "dash", "color": "#ffcf88", "text": "SP"},
        {"x": levels.get("mp"), "dash": "dot", "color": "#57b8ff", "text": "MP"},
        {"x": levels.get("flip"), "dash": "dot", "color": "#ffd166", "text": "GF"},
        {"x": levels.get("p1"), "dash": "dot", "color": "#36d081", "text": "P1"},
        {"x": levels.get("p2"), "dash": "dot", "color": "#36d081", "text": "P2"},
        {"x": levels.get("n1"), "dash": "dot", "color": "#ff6c6c", "text": "N1"},
        {"x": levels.get("n2"), "dash": "dot", "color": "#ff6c6c", "text": "N2"},
        {"x": levels.get("a1"), "dash": "dash", "color": "#ff9b26", "text": "A1"},
        {"x": levels.get("a2"), "dash": "dash", "color": "#ff9b26", "text": "A2"},
    ]
    line_levels = [item for item in line_levels if item["x"] is not None and pd.notna(item["x"])]

    strike_range = df["strike"].max() - df["strike"].min()
    overlap_threshold = max(float(strike_range) * 0.01, 1.0)
    sorted_levels = sorted(line_levels, key=lambda item: float(item["x"]))

    def stagger_xshift(level_idx: int) -> int:
        if level_idx == 0:
            return 0
        step = 10 * ((level_idx + 1) // 2)
        return step if level_idx % 2 else -step

    prev_x = None
    overlap_idx = 0
    for item in sorted_levels:
        x_val = float(item["x"])
        if prev_x is not None and abs(x_val - prev_x) <= overlap_threshold:
            overlap_idx += 1
        else:
            overlap_idx = 0
        fig.add_vline(
            x=x_val,
            line_dash=item["dash"],
            line_color=item["color"],
        )
        fig.add_annotation(
            x=x_val,
            y=1.0,
            xref="x",
            yref="paper",
            text=f"<b>{item['text']}</b>",
            showarrow=False,
            xanchor="center",
            yanchor="bottom",
            xshift=stagger_xshift(overlap_idx),
            yshift=8 + (overlap_idx * 14),
            bgcolor="rgba(8, 10, 14, 0.95)",
            bordercolor=item["color"],
            borderwidth=1,
            font={"size": 10, "color": "#f6f0d5"},
            opacity=0.96,
            align="center",
        )
        prev_x = x_val

    fig.update_layout(
        template="plotly_dark",
        bargap=0.14,
        legend_title_text="Metrics",
        barmode="overlay",
        paper_bgcolor="#05070a",
        plot_bgcolor="#0a0d10",
        font={"color": "#f6f0d5"},
        margin={"l": 52, "r": 22, "t": 50, "b": 45},
        xaxis={"title": "Strike", "gridcolor": "rgba(97, 108, 123, 0.22)", "zerolinecolor": "rgba(97, 108, 123, 0.22)"},
        yaxis={"title": "Net GEX (k)", "gridcolor": "rgba(97, 108, 123, 0.26)", "zerolinecolor": "rgba(97, 108, 123, 0.26)", "tickformat": ",.0f"},
        yaxis2={
            "title": "AG (M)",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "tickformat": ",.0f",
        },
    )

    left_vals = (df["total_gex"] / NET_GEX_SCALE).tolist()
    if left_vals:
        max_abs = max(abs(min(left_vals)), abs(max(left_vals)), 1.0)
        fig.update_layout(yaxis={"range": [-max_abs * 1.12, max_abs * 1.12]})
    ag_vals = (df["abs_gex"] / AG_SCALE).tolist()
    if ag_vals:
        ag_max = max(ag_vals)
        fig.update_layout(yaxis2={"range": [0, ag_max * 1.12 if ag_max > 0 else 1]})
    return fig


def build_spot_chart(levels: Dict) -> go.Figure:
    symbol = str(levels.get("symbol") or "BTC").upper()
    instrument = "BTC-PERPETUAL" if symbol == "BTC" else "ETH-PERPETUAL"
    timeframe = "15"
    lookback_minutes = max(
        DERIBIT_LOOKBACK_BY_RESOLUTION.get(str(timeframe), 360),
        int(DEFAULT_SESSION_BARS * resolution_to_minutes(timeframe) * 1.3),
    )
    candles = []
    try:
        candles = fetch_deribit_candles(
            instrument_name=instrument,
            resolution=str(timeframe),
            lookback_minutes=lookback_minutes,
        )
    except Exception:
        candles = []
    return build_spot_figure(
        history_points=[],
        levels=levels,
        deribit_candles=candles,
        max_bars=DEFAULT_SESSION_BARS,
        asset_symbol=symbol,
    )


def send_photo(chat_id: object, image_path: Path, caption: Optional[str] = None, reply_to: Optional[int] = None) -> bool:
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    if reply_to is not None:
        data["reply_to_message_id"] = reply_to
    with open(image_path, "rb") as handle:
        r = requests.post(
            f"{API_BASE}/sendPhoto",
            data=data,
            files={"photo": handle},
            timeout=30,
        )
    if not r.ok:
        return False
    payload = r.json() or {}
    return bool(payload.get("ok"))


def send_level_charts(
    primary_chat_id: object,
    extra_chat_ids: Sequence[object],
    reply_to: Optional[int],
    levels: Dict,
    gex_by_strike: pd.DataFrame,
    dff_filtered: Optional[pd.DataFrame],
) -> Optional[str]:
    if gex_by_strike is None or gex_by_strike.empty:
        return "Charts unavailable: no GEX data"
    try:
        gex_fig = build_gex_figure(levels, gex_by_strike)
        spot_fig = build_spot_chart(levels)
        heat_fig = None
        heat_summary = None
        if dff_filtered is not None and not dff_filtered.empty:
            heat_fig, heat_summary, _heat_card = build_option_heatmap_tool(dff_filtered)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            gex_path = tmp_path / f"{levels.get('symbol','BTC')}_gex.png"
            spot_path = tmp_path / f"{levels.get('symbol','BTC')}_spot.png"
            heat_path = tmp_path / f"{levels.get('symbol','BTC')}_heatmap.png"
            pio.write_image(gex_fig, gex_path, width=GEX_IMAGE_SIZE[0], height=GEX_IMAGE_SIZE[1], scale=2)
            pio.write_image(spot_fig, spot_path, width=SPOT_IMAGE_SIZE[0], height=SPOT_IMAGE_SIZE[1], scale=2)
            if heat_fig is not None:
                pio.write_image(heat_fig, heat_path, width=HEATMAP_IMAGE_SIZE[0], height=HEATMAP_IMAGE_SIZE[1], scale=2)

            send_photo(primary_chat_id, gex_path, caption="GEX by Strike", reply_to=reply_to)
            send_photo(primary_chat_id, spot_path, caption="Spot Candles + Levels")
            if heat_fig is not None:
                caption = "Options Heatmap"
                if heat_summary:
                    caption = f"Options Heatmap — {heat_summary}"
                send_photo(primary_chat_id, heat_path, caption=caption)
            for chat_id in extra_chat_ids:
                send_photo(chat_id, gex_path, caption="GEX by Strike")
                send_photo(chat_id, spot_path, caption="Spot Candles + Levels")
                if heat_fig is not None:
                    caption = "Options Heatmap"
                    if heat_summary:
                        caption = f"Options Heatmap — {heat_summary}"
                    send_photo(chat_id, heat_path, caption=caption)
    except Exception as exc:
        return f"Charts unavailable: {exc}"
    return None


def get_updates(offset: Optional[int], timeout: int = 30) -> List[Dict]:
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    r = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=timeout + 5)
    if r.status_code == 409:
        raise RuntimeError(
            "Telegram 409 conflict: another bot instance is polling or a webhook is set. "
            "Stop other bot processes or call deleteWebhook."
        )
    r.raise_for_status()
    payload = r.json() or {}
    if not payload.get("ok"):
        raise RuntimeError(payload)
    return payload.get("result", [])


def send_message(chat_id: object, text: str, reply_to: Optional[int] = None) -> bool:
    payload = {"chat_id": chat_id, "text": text}
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
    r = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
    if not r.ok:
        return False
    data = r.json() or {}
    return bool(data.get("ok"))


def send_message_with_keyboard(chat_id: object, text: str, keyboard: Dict, reply_to: Optional[int] = None) -> bool:
    payload = {"chat_id": chat_id, "text": text, "reply_markup": keyboard}
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
    r = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
    if not r.ok:
        return False
    data = r.json() or {}
    return bool(data.get("ok"))


def answer_callback(callback_id: str, text: Optional[str] = None) -> None:
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    try:
        requests.post(f"{API_BASE}/answerCallbackQuery", json=payload, timeout=5)
    except requests.RequestException:
        return


def build_levels_keyboard(include_broadcast: bool) -> Dict:
    rows = [
        [
            {"text": "BTC Weekly", "callback_data": "levels:BTC:weekly"},
            {"text": "ETH Weekly", "callback_data": "levels:ETH:weekly"},
        ],
        [
            {"text": "BTC Monthly", "callback_data": "levels:BTC:monthly"},
            {"text": "ETH Monthly", "callback_data": "levels:ETH:monthly"},
        ],
        [
            {"text": "BTC 4 Weekly", "callback_data": "levels:BTC:multi4"},
            {"text": "ETH 4 Weekly", "callback_data": "levels:ETH:multi4"},
        ],
    ]
    if include_broadcast:
        rows.append(
            [
                {"text": "Broadcast BTC Weekly", "callback_data": "levels:BTC:weekly:broadcast"},
                {"text": "Broadcast ETH Weekly", "callback_data": "levels:ETH:weekly:broadcast"},
            ]
        )
    return {"inline_keyboard": rows}


def parse_callback_data(data: str) -> Optional[List[str]]:
    if not data:
        return None
    if not data.startswith("levels:"):
        return None
    parts = data.split(":")
    if len(parts) < 3:
        return None
    symbol = parts[1].upper()
    window = parts[2].lower()
    args: List[str] = [symbol]
    if window in {"all", "*"}:
        args.append("all")
    elif window in {"weekly", "monthly"}:
        args.append(window)
    elif window.startswith("multi") and window[5:].isdigit():
        args.append(window)
    elif window.endswith("d") and window[:-1].isdigit():
        args.append(window)
    else:
        return None
    if len(parts) >= 4 and parts[3].lower() == "broadcast":
        args.append("broadcast")
    return args


def parse_menu_callback(data: str) -> Optional[List[str]]:
    if not data or not data.startswith("menu:"):
        return None
    parts = data.split(":")
    if len(parts) < 2:
        return None
    return parts[1:]


def build_symbol_keyboard() -> Dict:
    return {
        "inline_keyboard": [
            [
                {"text": "BTC", "callback_data": "menu:symbol:BTC"},
                {"text": "ETH", "callback_data": "menu:symbol:ETH"},
            ],
            [
                {"text": "ℹ️ Info", "callback_data": "menu:info"},
            ],
        ]
    }


def build_range_mode_keyboard(
    symbol: str,
    include_broadcast: bool,
    from_date: Optional[str],
    to_date: Optional[str],
) -> Dict:
    from_label = from_date or "Select"
    to_label = to_date or "Select"
    rows = [
        [
            {"text": f"From: {from_label}", "callback_data": f"range:pickfrom:{symbol}:0"},
        ],
        [
            {"text": f"To: {to_label}", "callback_data": f"range:pickto:{symbol}:0"},
        ],
        [
            {"text": "Weekly", "callback_data": f"levels:{symbol}:weekly"},
            {"text": "Monthly", "callback_data": f"levels:{symbol}:monthly"},
        ],
        [
            {"text": "Multi-weekly", "callback_data": f"menu:multi:{symbol}"},
            {"text": "All", "callback_data": f"levels:{symbol}:all"},
        ],
    ]
    if from_date and to_date:
        send_row = [{"text": "Send Range", "callback_data": f"range:send:{symbol}"}]
        if include_broadcast:
            send_row.append({"text": "Broadcast Range", "callback_data": f"range:send:{symbol}:broadcast"})
        rows.append(send_row)
    if include_broadcast:
        rows.append(
            [
                {"text": "Broadcast Weekly", "callback_data": f"levels:{symbol}:weekly:broadcast"},
                {"text": "Broadcast Monthly", "callback_data": f"levels:{symbol}:monthly:broadcast"},
            ]
        )
        rows.append(
            [
                {"text": "Broadcast All", "callback_data": f"levels:{symbol}:all:broadcast"},
            ]
        )
    rows.append(
        [
            {"text": "ℹ️ Info", "callback_data": "menu:info"},
            {"text": "Back", "callback_data": "menu:back:symbol"},
        ]
    )
    return {"inline_keyboard": rows}


def build_multi_keyboard(symbol: str) -> Dict:
    rows = [
        [
            {"text": "2 Weeklies", "callback_data": f"levels:{symbol}:multi2"},
            {"text": "3 Weeklies", "callback_data": f"levels:{symbol}:multi3"},
        ],
        [
            {"text": "4 Weeklies", "callback_data": f"levels:{symbol}:multi4"},
            {"text": "6 Weeklies", "callback_data": f"levels:{symbol}:multi6"},
        ],
        [
            {"text": "ℹ️ Info", "callback_data": "menu:info"},
            {"text": "Back", "callback_data": f"menu:back:range:{symbol}"},
        ],
    ]
    return {"inline_keyboard": rows}


def send_levels_menu(chat_id: object, reply_to: Optional[int] = None) -> None:
    include_broadcast = bool(TELEGRAM_CHANNEL_ID)
    keyboard = build_levels_keyboard(include_broadcast=include_broadcast)
    send_message_with_keyboard(
        chat_id,
        "Choose a levels request:",
        keyboard,
        reply_to=reply_to,
    )


def send_symbol_menu(chat_id: object, reply_to: Optional[int] = None) -> None:
    send_message_with_keyboard(
        chat_id,
        "Choose a symbol:",
        build_symbol_keyboard(),
        reply_to=reply_to,
    )


def send_range_menu(chat_id: object, symbol: str, reply_to: Optional[int] = None) -> None:
    include_broadcast = bool(TELEGRAM_CHANNEL_ID)
    state = RANGE_STATE.get(int(chat_id), {})
    from_date = state.get("from")
    to_date = state.get("to")
    keyboard = build_range_mode_keyboard(
        symbol=symbol,
        include_broadcast=include_broadcast,
        from_date=from_date,
        to_date=to_date,
    )
    send_message_with_keyboard(
        chat_id,
        f"Choose a range for {symbol}:",
        keyboard,
        reply_to=reply_to,
    )


def send_multi_menu(chat_id: object, symbol: str, reply_to: Optional[int] = None) -> None:
    keyboard = build_multi_keyboard(symbol=symbol)
    send_message_with_keyboard(
        chat_id,
        f"Choose how many weekly expiries for {symbol}:",
        keyboard,
        reply_to=reply_to,
    )


def available_expiries(symbol: str, exchanges: Sequence[str]) -> List[str]:
    df = load_options_frame()
    dff = df[df["symbol"] == symbol]
    if exchanges:
        dff = dff[dff["exchange"].isin(exchanges)]
    if dff.empty:
        return []
    today = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    forward = dff[dff["expiry"] >= today]
    expiries = sorted({pd.Timestamp(x).strftime("%Y-%m-%d") for x in forward["expiry"].dropna().tolist()})
    return expiries


def build_expiry_picker_keyboard(symbol: str, mode: str, expiries: List[str], page: int) -> Dict:
    total = len(expiries)
    page = max(0, int(page))
    start = page * EXPIRY_PAGE_SIZE
    end = min(start + EXPIRY_PAGE_SIZE, total)
    page_items = expiries[start:end]

    rows: List[List[Dict[str, str]]] = []
    row: List[Dict[str, str]] = []
    for item in page_items:
        row.append({"text": item, "callback_data": f"range:{mode}:{symbol}:{item}"})
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav_row: List[Dict[str, str]] = []
    if start > 0:
        nav_row.append({"text": "⬅️ Prev", "callback_data": f"range:pick{mode}:{symbol}:{page - 1}"})
    if end < total:
        nav_row.append({"text": "Next ➡️", "callback_data": f"range:pick{mode}:{symbol}:{page + 1}"})
    if nav_row:
        rows.append(nav_row)

    rows.append([{"text": "Back", "callback_data": f"range:back:{symbol}"}])
    return {"inline_keyboard": rows}


def send_range_picker(chat_id: object, symbol: str, mode: str, page: int, reply_to: Optional[int] = None) -> None:
    exchanges = parse_exchange_list(TELEGRAM_DEFAULT_EXCHANGES)
    expiries = available_expiries(symbol, exchanges)
    if mode == "to":
        state = RANGE_STATE.get(int(chat_id), {})
        from_date = state.get("from")
        if from_date:
            expiries = [d for d in expiries if d >= from_date]
    if not expiries:
        send_message(chat_id, "No available expiries for that selection.", reply_to=reply_to)
        return
    keyboard = build_expiry_picker_keyboard(symbol, mode, expiries, page)
    title = f"Select {'From' if mode == 'from' else 'To'} date for {symbol}:"
    send_message_with_keyboard(chat_id, title, keyboard, reply_to=reply_to)


def send_info(chat_id: object, reply_to: Optional[int] = None) -> None:
    send_message(chat_id, INFO_TEXT, reply_to=reply_to)


def _format_legs(ticket: Dict) -> str:
    legs = ticket.get("legs") or []
    if not legs:
        return "n/a"
    parts = []
    for leg in legs:
        side = str(leg.get("side", "")).upper()[:1]
        opt_type = "C" if str(leg.get("type", "")).lower().startswith("c") else "P"
        strike = fmt_price(leg.get("strike"))
        parts.append(f"{side}{strike}{opt_type}")
    return " ".join(parts)


def _ticket_quantity(ticket: Dict) -> int:
    qty = ticket.get("quantity")
    if qty is None:
        legs = ticket.get("legs") or []
        if legs:
            qty = legs[0].get("quantity")
    try:
        return int(qty)
    except (TypeError, ValueError):
        return 0


def format_trade_ideas(payload: Dict) -> str:
    ideas = payload.get("ideas") or []
    if not ideas:
        reason = payload.get("reason", "no ideas")
        return f"💡 Trade Ideas\nNo ideas available ({reason})."

    lines = ["💡 Trade Ideas"]
    shown = 0
    for idx, idea in enumerate(ideas[:3], start=1):
        name = idea.get("name", "Strategy")
        expiry = idea.get("expiry", "n/a")
        conviction = idea.get("conviction", 0)
        rr = idea.get("rr")
        max_profit = idea.get("max_profit")
        max_loss = idea.get("max_loss")
        rationale = idea.get("rationale", "")
        hedge = idea.get("hedge", "")
        ticket = idea.get("ticket", {})
        qty = _ticket_quantity(ticket)
        if qty <= 0:
            continue
        legs = _format_legs(ticket)

        header = f"{idx}) {name} | Exp {expiry} | Qty {qty} | Conv {int(conviction)}/100"
        rr_text = f"RR {rr:.2f}" if rr is not None and pd.notna(rr) else "RR n/a"
        risk_text = f"Max {fmt_money(max_profit)}/{fmt_money(max_loss)}"
        lines.append(header)
        lines.append(f"   {rr_text} | {risk_text}")
        lines.append(f"   Legs: {legs}")
        if rationale:
            lines.append(f"   Why: {rationale}")
        if hedge:
            lines.append(f"   Hedge: {hedge}")
        shown += 1
    if shown == 0:
        reason = payload.get("reason", "quantity=0")
        return f"💡 Trade Ideas\nNo tradable ideas ({reason})."
    return "\n".join(lines)


def send_trade_ideas(chat_id: object, reply_to: Optional[int] = None) -> None:
    context = LAST_REQUESTS.get(int(chat_id)) if chat_id is not None else None
    if not context:
        send_message(chat_id, "💡 No recent levels request found. Use /start and request levels first.", reply_to=reply_to)
        return
    symbol = str(context.get("symbol") or "BTC").upper()
    exchanges = context.get("exchanges") or []
    expiries = context.get("expiries") or []

    try:
        df = load_options_frame()
    except Exception as exc:
        send_message(chat_id, f"Trade ideas unavailable: {exc}", reply_to=reply_to)
        return

    dff = df[df["symbol"] == symbol]
    if exchanges:
        dff = dff[dff["exchange"].isin(exchanges)]
    if expiries:
        exp_set = set(expiries)
        dff = dff[dff["expiry"].dt.strftime("%Y-%m-%d").isin(exp_set)]

    try:
        payload = generate_professional_ideas(dff, symbol=symbol, account_equity=ACCOUNT_EQUITY_USD)
    except Exception as exc:
        send_message(chat_id, f"Trade ideas unavailable: {exc}", reply_to=reply_to)
        return

    message = format_trade_ideas(payload)
    send_message(chat_id, message, reply_to=reply_to)


def delete_webhook() -> None:
    try:
        r = requests.get(
            f"{API_BASE}/deleteWebhook",
            params={"drop_pending_updates": True},
            timeout=10,
        )
        if not r.ok:
            print(f"[telegram] deleteWebhook failed: {r.status_code} {r.text}")
    except requests.RequestException as exc:
        print(f"[telegram] deleteWebhook error: {exc}")


def handle_levels_request(chat_id: int, message_id: Optional[int], args: Sequence[str]) -> None:
    broadcast = False
    filtered_args = []
    for raw in args:
        token = str(raw).strip().lower()
        if token in {"broadcast", "channel", "post"}:
            broadcast = True
        else:
            filtered_args.append(raw)

    symbol, days, _use_all, mode, multi_count = parse_levels_args(filtered_args)
    exchanges = parse_exchange_list(TELEGRAM_DEFAULT_EXCHANGES)

    try:
        df = load_options_frame()
    except Exception as exc:
        send_message(chat_id, f"Levels unavailable: {exc}", reply_to=message_id)
        return

    levels, note, gex_by_strike, dff_filtered, window_label = compute_levels(
        df,
        symbol=symbol,
        exchanges=exchanges,
        days=days,
        mode=mode,
        multi_count=multi_count,
    )
    if not levels:
        send_message(chat_id, f"Levels unavailable: {note or 'no data'}", reply_to=message_id)
        return

    deliver_levels(
        chat_id=chat_id,
        message_id=message_id,
        levels=levels,
        note=note,
        gex_by_strike=gex_by_strike,
        dff_filtered=dff_filtered,
        window_label=window_label,
        symbol=symbol,
        exchanges=exchanges,
        broadcast=broadcast,
    )


def handle_range_request(
    chat_id: int,
    message_id: Optional[int],
    symbol: str,
    start_date: str,
    end_date: str,
    broadcast: bool = False,
) -> None:
    exchanges = parse_exchange_list(TELEGRAM_DEFAULT_EXCHANGES)
    try:
        df = load_options_frame()
    except Exception as exc:
        send_message(chat_id, f"Levels unavailable: {exc}", reply_to=message_id)
        return

    levels, note, gex_by_strike, dff_filtered, window_label = compute_levels(
        df,
        symbol=symbol,
        exchanges=exchanges,
        days=None,
        mode=None,
        multi_count=None,
        start_expiry=start_date,
        end_expiry=end_date,
    )
    if not levels:
        send_message(chat_id, f"Levels unavailable: {note or 'no data'}", reply_to=message_id)
        return

    deliver_levels(
        chat_id=chat_id,
        message_id=message_id,
        levels=levels,
        note=note,
        gex_by_strike=gex_by_strike,
        dff_filtered=dff_filtered,
        window_label=window_label,
        symbol=symbol,
        exchanges=exchanges,
        broadcast=broadcast,
    )


def deliver_levels(
    chat_id: int,
    message_id: Optional[int],
    levels: Dict,
    note: Optional[str],
    gex_by_strike: Optional[pd.DataFrame],
    dff_filtered: Optional[pd.DataFrame],
    window_label: str,
    symbol: str,
    exchanges: Sequence[str],
    broadcast: bool,
) -> None:
    message = format_levels_message(levels, note, window_label)
    send_message(chat_id, message, reply_to=message_id)

    if dff_filtered is not None and not dff_filtered.empty:
        expiries = sorted({pd.Timestamp(x).strftime("%Y-%m-%d") for x in dff_filtered["expiry"].dropna().tolist()})
        LAST_REQUESTS[int(chat_id)] = {
            "symbol": symbol,
            "exchanges": list(exchanges),
            "expiries": expiries,
        }

    extra_chats: List[object] = []
    if broadcast and TELEGRAM_CHANNEL_ID:
        send_message(TELEGRAM_CHANNEL_ID, message)
        extra_chats.append(TELEGRAM_CHANNEL_ID)

    if gex_by_strike is not None:
        chart_error = send_level_charts(chat_id, extra_chats, message_id, levels, gex_by_strike, dff_filtered)
        if chart_error:
            send_message(chat_id, chart_error, reply_to=message_id)

    send_message_with_keyboard(
        chat_id,
        "💡 Want trade ideas for this window?",
        {"inline_keyboard": [[{"text": "💡 Trading Ideas", "callback_data": "ideas"}]]},
    )


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is not configured")

    allowed = parse_allowed_chat_ids(TELEGRAM_ALLOWED_CHAT_IDS)
    offset: Optional[int] = None

    delete_webhook()

    while True:
        try:
            updates = get_updates(offset=offset, timeout=30)
        except Exception as exc:
            print(f"Polling error: {exc}")
            time.sleep(3)
            continue

        for update in updates:
            offset = update.get("update_id", 0) + 1
            callback = update.get("callback_query")
            if callback:
                callback_id = callback.get("id")
                message = callback.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = chat.get("id")
                if not is_allowed_chat(chat_id, allowed):
                    if callback_id:
                        answer_callback(callback_id, "Not authorized")
                    continue
                data = callback.get("data") or ""
                if data == "ideas":
                    if callback_id:
                        answer_callback(callback_id, "Generating ideas...")
                    send_trade_ideas(chat_id, reply_to=message.get("message_id"))
                    continue
                if data.startswith("range:"):
                    parts = data.split(":")
                    action = parts[1] if len(parts) > 1 else ""
                    symbol = parts[2] if len(parts) > 2 else ""
                    if action in {"pickfrom", "pickto"} and len(parts) >= 4:
                        try:
                            page = int(parts[3])
                        except ValueError:
                            page = 0
                        if callback_id:
                            answer_callback(callback_id, "Pick date")
                        mode = "from" if action == "pickfrom" else "to"
                        send_range_picker(chat_id, symbol, mode, page, reply_to=message.get("message_id"))
                        continue
                    if action in {"from", "to"} and len(parts) >= 4:
                        date_value = parts[3]
                        state = RANGE_STATE.setdefault(int(chat_id), {"symbol": symbol, "from": None, "to": None})
                        if action == "from":
                            state["from"] = date_value
                            if state.get("to") and state["to"] < date_value:
                                state["to"] = None
                        else:
                            state["to"] = date_value
                        if callback_id:
                            answer_callback(callback_id, f"{action.title()} set")
                        send_range_menu(chat_id, symbol, reply_to=message.get("message_id"))
                        continue
                    if action == "send" and len(parts) >= 3:
                        state = RANGE_STATE.get(int(chat_id), {})
                        start_date = state.get("from")
                        end_date = state.get("to")
                        broadcast = len(parts) >= 4 and parts[3].lower() == "broadcast"
                        if not start_date or not end_date:
                            if callback_id:
                                answer_callback(callback_id, "Select From + To first")
                            send_range_menu(chat_id, symbol, reply_to=message.get("message_id"))
                            continue
                        if callback_id:
                            answer_callback(callback_id, "Fetching range...")
                        handle_range_request(chat_id, message.get("message_id"), symbol, start_date, end_date, broadcast=broadcast)
                        continue
                    if action == "back" and len(parts) >= 3:
                        if callback_id:
                            answer_callback(callback_id, "Back")
                        send_range_menu(chat_id, symbol, reply_to=message.get("message_id"))
                        continue
                menu = parse_menu_callback(data)
                if menu:
                    action = menu[0]
                    rest = menu[1:]
                    if action == "symbol" and rest and rest[0] in {"BTC", "ETH"}:
                        symbol = rest[0]
                        if callback_id:
                            answer_callback(callback_id, f"{symbol} selected")
                        RANGE_STATE[int(chat_id)] = {"symbol": symbol, "from": None, "to": None}
                        send_range_menu(chat_id, symbol, reply_to=message.get("message_id"))
                    elif action == "multi" and rest and rest[0] in {"BTC", "ETH"}:
                        symbol = rest[0]
                        if callback_id:
                            answer_callback(callback_id, "Multi-weekly")
                        send_multi_menu(chat_id, symbol, reply_to=message.get("message_id"))
                    elif action == "info":
                        if callback_id:
                            answer_callback(callback_id, "Info")
                        send_info(chat_id, reply_to=message.get("message_id"))
                    elif action == "back":
                        target = rest[0] if rest else "symbol"
                        if target == "range" and len(rest) >= 2 and rest[1] in {"BTC", "ETH"}:
                            symbol = rest[1]
                            if callback_id:
                                answer_callback(callback_id, "Back")
                            send_range_menu(chat_id, symbol, reply_to=message.get("message_id"))
                        else:
                            if callback_id:
                                answer_callback(callback_id, "Back")
                            send_symbol_menu(chat_id, reply_to=message.get("message_id"))
                    elif action == "menu":
                        if callback_id:
                            answer_callback(callback_id, "Menu")
                        send_symbol_menu(chat_id, reply_to=message.get("message_id"))
                    else:
                        if callback_id:
                            answer_callback(callback_id, "Unknown action")
                    continue
                args = parse_callback_data(data)
                if args is None:
                    if callback_id:
                        answer_callback(callback_id, "Unknown action")
                    continue
                if callback_id:
                    answer_callback(callback_id, "Fetching levels...")
                handle_levels_request(chat_id, message.get("message_id"), args)
                continue

            message = update.get("message") or update.get("edited_message")
            if not message:
                continue
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            if not is_allowed_chat(chat_id, allowed):
                continue
            text = (message.get("text") or "").strip()
            if not text:
                continue

            args = parse_levels_command(text)
            if args is None:
                continue
            if args and args[0] == "__help__":
                send_message(chat_id, HELP_TEXT, reply_to=message.get("message_id"))
                continue
            if args and args[0] == "__menu__":
                send_symbol_menu(chat_id, reply_to=message.get("message_id"))
                continue
            if not args:
                send_symbol_menu(chat_id, reply_to=message.get("message_id"))
                continue

            handle_levels_request(chat_id, message.get("message_id"), args)

        time.sleep(0.2)


if __name__ == "__main__":
    main()
