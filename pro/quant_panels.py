"""Bokeh figure builders for Tier-1 quant panels.

These return raw Bokeh figures; the dashboard layer is responsible for
applying its dark theme + serializing to HTML for an iframe.

Panels:
- Intraday GEX time series         build_intraday_gex_figure(metric_history_df, snapshot_history_df)
- Vol surface (strike x expiry)    build_vol_surface_figure(chain_df, spot)
- Realized vs Implied volatility   build_rv_iv_figure(candles_df, atm_iv, atm_iv_history)
- Dealer hedge flow backtest       build_hedge_backtest_figure(snapshot_history_df)
"""
from __future__ import annotations

from typing import List, Optional

import math
import numpy as np
import pandas as pd

from bokeh.plotting import figure as bokeh_figure
from bokeh.models import (
    ColumnDataSource,
    DatetimeTickFormatter,
    HoverTool,
    LinearAxis,
    LinearColorMapper,
    ColorBar,
    NumeralTickFormatter,
    Range1d,
    Span,
    BasicTicker,
)


# ─────────────────────────── helpers ────────────────────────────

def _empty(p_height: int, message: str):
    p = bokeh_figure(height=p_height, sizing_mode="stretch_width", toolbar_location=None)
    p.text(x=[0.5], y=[0.5], text=[message],
           text_color="#7d8597", text_align="center",
           text_baseline="middle", text_font_size="11px")
    p.xaxis.visible = False
    p.yaxis.visible = False
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    return p


def _datetime_axis(p):
    p.xaxis.formatter = DatetimeTickFormatter(
        hours="%H:%M",
        days="%m-%d",
        months="%Y-%m",
    )


# ─────────────────────── 1. Intraday GEX ──────────────────────────

def build_intraday_gex_figure(metric_df: pd.DataFrame, snapshot_df: Optional[pd.DataFrame] = None):
    """Net GEX over time + spot price overlay (right axis).

    metric_df: from snapshot_store.load_metric_history (cols: ts_utc, net_gex)
    snapshot_df: from snapshot_store.load_snapshot_range (cols: ts_utc, spot_price)
    """
    if metric_df is None or metric_df.empty:
        return _empty(280, "No GEX history yet — collector needs to run.")

    df = metric_df.copy().sort_values("ts_utc")
    if df["ts_utc"].dt.tz is None:
        df["ts_utc"] = df["ts_utc"].dt.tz_localize("UTC")

    spot_series = pd.DataFrame()
    if snapshot_df is not None and not snapshot_df.empty:
        s = snapshot_df.copy().sort_values("ts_utc")
        if s["ts_utc"].dt.tz is None:
            s["ts_utc"] = s["ts_utc"].dt.tz_localize("UTC")
        spot_series = s.groupby("ts_utc", as_index=False)["spot_price"].last()

    src_gex = ColumnDataSource(dict(
        t=df["ts_utc"],
        net_gex=df["net_gex"].astype(float),
    ))

    gex_max = float(df["net_gex"].abs().max() or 1.0)
    gex_pad = gex_max * 0.18

    p = bokeh_figure(
        height=280,
        sizing_mode="stretch_width",
        x_axis_type="datetime",
        y_axis_label="Net GEX ($)",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        toolbar_location="right",
    )
    p.y_range = Range1d(start=-gex_max - gex_pad, end=gex_max + gex_pad)

    # Zero baseline
    p.add_layout(Span(location=0, dimension="width",
                      line_color="#8a92a6", line_alpha=0.4,
                      line_width=1, line_dash="dashed"))

    p.line("t", "net_gex", source=src_gex,
           line_color="#13b955", line_width=2, legend_label="Net GEX")
    p.scatter("t", "net_gex", source=src_gex,
              size=4, color="#13b955", alpha=0.7)

    # Spot overlay on right axis
    if not spot_series.empty:
        spot_min = float(spot_series["spot_price"].min())
        spot_max = float(spot_series["spot_price"].max())
        spot_span = max(spot_max - spot_min, max(abs(spot_max), 1.0) * 0.001)
        spot_pad = spot_span * 0.1
        p.extra_y_ranges = {"spot": Range1d(start=spot_min - spot_pad, end=spot_max + spot_pad)}
        p.add_layout(LinearAxis(y_range_name="spot", axis_label="Spot ($)"), "right")
        p.line(spot_series["ts_utc"], spot_series["spot_price"],
               y_range_name="spot", line_color="#5b8dea",
               line_width=1.6, line_dash="solid", legend_label="Spot")

    p.yaxis[0].formatter = NumeralTickFormatter(format="$0,0.[0]a")
    _datetime_axis(p)
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.legend.click_policy = "hide"

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [("Time", "@t{%F %H:%M}"), ("Net GEX", "@net_gex{$0,0.[0]a}")]
        hover.formatters = {"@t": "datetime"}

    return p


# ─────────────────────── 2. Vol surface ───────────────────────────

def build_vol_surface_figure(chain_df: pd.DataFrame, spot: Optional[float]):
    """Heatmap of implied vol across strikes (y) × expiries (x).

    Uses mid IV from each option (averaged across call/put per strike/expiry).
    """
    if chain_df is None or chain_df.empty:
        return _empty(360, "No live chain — vol surface unavailable.")

    df = chain_df.copy()
    df = df[pd.notna(df.get("iv"))]
    if df.empty:
        return _empty(360, "Chain has no per-strike IV.")
    df["iv"] = df["iv"].astype(float)
    df = df[df["iv"] > 0]
    if df.empty:
        return _empty(360, "No positive IV values in chain.")

    # Trim wings to ±35% from spot for readability
    if spot and spot > 0:
        lo = float(spot) * 0.65
        hi = float(spot) * 1.35
        df = df[(df["strike"] >= lo) & (df["strike"] <= hi)]
        if df.empty:
            return _empty(360, "No strikes within ±35% of spot.")

    grouped = df.groupby(["expiry", "strike"], as_index=False).agg(iv=("iv", "mean"))
    grouped["expiry_str"] = grouped["expiry"].astype(str)
    grouped["iv_pct"] = grouped["iv"] * 100.0

    expiries = sorted(grouped["expiry_str"].unique(),
                      key=lambda s: pd.to_datetime(s, errors="coerce"))
    strikes = sorted(grouped["strike"].unique())
    strike_labels = [f"{int(round(s)):,}" for s in strikes]
    strike_lookup = {s: lbl for s, lbl in zip(strikes, strike_labels)}
    grouped["strike_label"] = grouped["strike"].map(strike_lookup)

    iv_min = float(grouped["iv_pct"].min())
    iv_max = float(grouped["iv_pct"].max())
    if iv_max <= iv_min:
        iv_max = iv_min + 1.0
    palette = [
        "#0c2444", "#103962", "#114e80", "#13629d", "#1a78ba",
        "#3a8fc3", "#62a6c5", "#94bcc1", "#cdcfb6", "#f0c997",
        "#f0a874", "#ec8552", "#dd5f3a", "#c5402d", "#a52729",
    ]
    mapper = LinearColorMapper(palette=palette, low=iv_min, high=iv_max)

    src = ColumnDataSource(dict(
        expiry=grouped["expiry_str"],
        strike=grouped["strike_label"],
        strike_raw=grouped["strike"],
        iv=grouped["iv_pct"],
    ))

    p = bokeh_figure(
        height=360,
        sizing_mode="stretch_width",
        x_range=expiries,
        y_range=strike_labels,
        x_axis_label="Expiry",
        y_axis_label="Strike",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        toolbar_location="right",
    )
    p.rect(
        x="expiry", y="strike", source=src,
        width=1.0, height=1.0,
        fill_color={"field": "iv", "transform": mapper},
        line_color=None,
    )

    color_bar = ColorBar(
        color_mapper=mapper,
        ticker=BasicTicker(desired_num_ticks=6),
        formatter=NumeralTickFormatter(format="0.0\\%"),
        label_standoff=6, border_line_color=None,
        background_fill_color="#141720",
        major_label_text_color="#8a92a6",
    )
    p.add_layout(color_bar, "right")

    if spot and spot > 0:
        spot_label = strike_lookup.get(min(strikes, key=lambda s: abs(s - float(spot))))
        if spot_label:
            p.add_layout(Span(location=spot_label, dimension="width",
                              line_color="#ffffff", line_width=1.4,
                              line_dash="dashed", line_alpha=0.7))

    p.xaxis.major_label_orientation = 0.7
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Expiry", "@expiry"),
            ("Strike", "@strike_raw{0,0}"),
            ("IV", "@iv{0.0}%"),
        ]
    return p


# ───────────────────── 3. Realized vs Implied ─────────────────────

def parkinson_vol(candles_df: pd.DataFrame, window: int = 30) -> pd.Series:
    """Annualized Parkinson volatility from OHLC candles."""
    if candles_df is None or candles_df.empty:
        return pd.Series(dtype=float)
    df = candles_df.copy()
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df = df.dropna(subset=["high", "low"])
    if df.empty:
        return pd.Series(dtype=float)
    log_hl = np.log(df["high"] / df["low"])
    factor = 1.0 / (4.0 * math.log(2.0))
    rolling = (log_hl ** 2).rolling(window=window, min_periods=max(5, window // 3)).mean()
    park = np.sqrt(factor * rolling)
    # Approximate periods/year for any candle resolution from median dt
    if "t" in df.columns and len(df) >= 2:
        deltas = pd.to_datetime(df["t"]).diff().dt.total_seconds().dropna()
        med_secs = float(deltas.median()) if not deltas.empty else 60.0
    else:
        med_secs = 60.0
    periods_per_year = (365.0 * 24.0 * 3600.0) / max(med_secs, 1.0)
    return park * math.sqrt(periods_per_year)


def build_rv_iv_figure(candles_df: pd.DataFrame, atm_iv: Optional[float],
                       window: int = 30):
    """Rolling realized vol vs ATM IV (single line). Returns Bokeh fig."""
    if candles_df is None or candles_df.empty:
        return _empty(280, "No candles — realized vol unavailable.")

    df = candles_df.copy()
    if "t" not in df.columns:
        return _empty(280, "Candles missing timestamp column.")
    df["t"] = pd.to_datetime(df["t"], utc=True)
    df = df.sort_values("t")

    rv = parkinson_vol(df, window=window) * 100.0
    if rv.dropna().empty:
        return _empty(280, "Not enough candles to compute realized vol.")

    p = bokeh_figure(
        height=280,
        sizing_mode="stretch_width",
        x_axis_type="datetime",
        y_axis_label="Volatility (%)",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        toolbar_location="right",
    )
    p.line(df["t"], rv, line_color="#e8a93b", line_width=2,
           legend_label=f"Parkinson RV ({window})")

    if atm_iv is not None and atm_iv > 0:
        iv_pct = float(atm_iv) * 100.0
        p.line(
            [df["t"].iloc[0], df["t"].iloc[-1]],
            [iv_pct, iv_pct],
            line_color="#5b8dea", line_width=2,
            line_dash="dashed",
            legend_label=f"ATM IV ({iv_pct:.0f}%)",
        )

    p.yaxis[0].formatter = NumeralTickFormatter(format="0.[0]")
    _datetime_axis(p)
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.legend.click_policy = "hide"

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [("Time", "@x{%F %H:%M}"), ("Vol %", "@y{0.0}")]
        hover.formatters = {"@x": "datetime"}
    return p


def vol_risk_premium(rv_series: pd.Series, atm_iv: Optional[float]) -> Optional[float]:
    """IV − RV (in %), positive = premium rich, negative = premium cheap."""
    if atm_iv is None or atm_iv <= 0:
        return None
    rv_clean = rv_series.dropna()
    if rv_clean.empty:
        return None
    last_rv_pct = float(rv_clean.iloc[-1]) * 100.0
    return float(atm_iv * 100.0) - last_rv_pct


# ────────────────── 4. Dealer hedge flow backtest ──────────────────

def hedge_flow_backtest(snapshot_history_df: pd.DataFrame) -> pd.DataFrame:
    """Compare predicted dealer hedge direction (sign of -NetGEX × dS)
    with actual realized spot move on the next snapshot.

    Returns df with columns: t, net_gex, spot, ret_fwd, predicted_dir, hit
    """
    if snapshot_history_df is None or snapshot_history_df.empty:
        return pd.DataFrame()

    df = snapshot_history_df.copy()
    if df["ts_utc"].dt.tz is None:
        df["ts_utc"] = df["ts_utc"].dt.tz_localize("UTC")

    by_time = df.groupby("ts_utc", as_index=False).agg(
        net_gex=("total_gex", "sum"),
        spot=("spot_price", "last"),
    ).sort_values("ts_utc")
    if len(by_time) < 3:
        return pd.DataFrame()

    by_time["ret_fwd"] = by_time["spot"].shift(-1) / by_time["spot"].replace(0.0, np.nan) - 1.0
    # Mean-reverting (NetGEX>0) → expected next move smaller / opposite to last imbalance.
    # We use sign of net_gex: positive => expect small/negative drift (regime suppresses moves);
    # negative => expect drift in same direction as flow (trend amplification).
    by_time["predicted_dir"] = np.where(by_time["net_gex"] >= 0, -1.0, 1.0)
    by_time["actual_dir"] = np.sign(by_time["ret_fwd"]).fillna(0.0)
    by_time["hit"] = (by_time["predicted_dir"] == by_time["actual_dir"]).astype(int)
    by_time = by_time.dropna(subset=["ret_fwd"])
    return by_time


def build_hedge_backtest_figure(snapshot_history_df: pd.DataFrame):
    """Cumulative hit-rate of regime predictor + spot drift overlay."""
    bt = hedge_flow_backtest(snapshot_history_df)
    if bt.empty:
        return _empty(280, "Need ≥3 snapshots to backtest hedge flow."), None

    bt = bt.reset_index(drop=True)
    bt["cum_hit_rate"] = bt["hit"].expanding().mean() * 100.0

    p = bokeh_figure(
        height=280,
        sizing_mode="stretch_width",
        x_axis_type="datetime",
        y_axis_label="Hit Rate (%)",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        toolbar_location="right",
    )
    p.y_range = Range1d(start=0, end=100)

    # 50% baseline
    p.add_layout(Span(location=50, dimension="width",
                      line_color="#8a92a6", line_alpha=0.4,
                      line_width=1, line_dash="dashed"))

    p.line(bt["ts_utc"], bt["cum_hit_rate"],
           line_color="#13b955", line_width=2,
           legend_label="Cumulative hit rate")
    p.scatter(bt["ts_utc"], bt["cum_hit_rate"],
              size=4, color="#13b955", alpha=0.7)

    # Spot on right axis
    spot_min = float(bt["spot"].min())
    spot_max = float(bt["spot"].max())
    if spot_max > spot_min:
        spot_pad = (spot_max - spot_min) * 0.08
        p.extra_y_ranges = {"spot": Range1d(start=spot_min - spot_pad, end=spot_max + spot_pad)}
        p.add_layout(LinearAxis(y_range_name="spot", axis_label="Spot"), "right")
        p.line(bt["ts_utc"], bt["spot"], y_range_name="spot",
               line_color="#5b8dea", line_width=1.4,
               line_dash="solid", legend_label="Spot")

    _datetime_axis(p)
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.legend.click_policy = "hide"

    summary = {
        "samples": int(len(bt)),
        "hit_rate": float(bt["hit"].mean()),
        "last_pred": "Mean-Reverting" if bt["predicted_dir"].iloc[-1] < 0 else "Trend-Following",
    }
    return p, summary
