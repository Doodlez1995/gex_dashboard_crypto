import warnings
warnings.filterwarnings("ignore", message="no explicit representation of timezones")

import dash
from dash import dcc, html, Input, Output, State, ALL
import plotly.graph_objects as go
from pathlib import Path
from bokeh.plotting import figure as bokeh_figure
from bokeh.embed import file_html
from bokeh.resources import CDN
from bokeh.models import (
    HoverTool, CrosshairTool, Span, Label, LabelSet, ColumnDataSource,
    LinearColorMapper, ColorBar, NumeralTickFormatter,
    DatetimeTickFormatter, Range1d, LinearAxis,
)
import pandas as pd
import urllib.parse
import urllib.request
import json
from config import (
    OPTIONS_FILE,
    SNAPSHOT_DB,
    ACCOUNT_EQUITY_USD,
    TELEGRAM_CHANNEL_URL,
    TELEGRAM_BOT_HANDLE,
    ALERT_RULES_FILE,
    ALERT_THROTTLE_MIN,
    ALERT_CHANNELS,
    ALERT_WEBHOOK_URL,
    POSITIONS_FILE,
    DATA_DIR,
)
from pro.snapshot_store import (
    write_metric,
    load_metric_history,
    write_alert,
    load_alerts,
    ack_alerts,
    get_last_alert_ts,
    load_snapshot_at,
    load_snapshot_timestamps,
    load_snapshot_range,
    load_latest_metric,
)
from pro.alerts import evaluate_rules as evaluate_alert_rules
from pro.monitoring import health_report, send_webhook_alert
from pro.portfolio import load_positions, normalize_positions, POSITION_COLUMNS, build_portfolio_snapshot
from pro.strategy_suite import (
    DEFAULT_COMMISSION_PER_CONTRACT,
    DEFAULT_STRATEGY_TEMPLATES,
    build_optimizer_candidates,
    default_builder_legs,
    delete_strategy,
    evaluate_strategy,
    fetch_deribit_option_chain,
    fetch_deribit_options_flow,
    get_chain_spot,
    list_expiries,
    list_strikes,
    load_saved_strategies,
    nearest_strike,
    normalize_builder_legs,
    option_chain_from_store,
    option_chain_store_data,
    save_strategy,
    template_label,
)
from pro.strategies import generate_professional_ideas
from pro.backtest import run_walk_forward_backtest
from pro.options_backtest import run_options_backtest, suggest_next_trade, STRATEGY_TYPES, CYCLE_TYPES, DEFAULT_DELTAS
from pro.cache import TTLCache
from pro.deribit_client import DeribitClient
from pro.volatility import estimate_term_iv, classify_vol_regime
from pro.greeks import (
    compute_chain_exposures,
    aggregate_by_strike,
    compute_dealer_hedge_flow,
)
from pro.quant_panels import (
    build_intraday_gex_figure,
    build_vol_surface_figure,
    build_rv_iv_figure,
    build_hedge_backtest_figure,
    parkinson_vol,
    vol_risk_premium,
)

THEME_CSS = """
:root {
    --bbg-bg: #0b0e14;
    --bbg-bg-alt: #11151d;
    --bbg-surface: #141821;
    --bbg-panel: #131722;
    --bbg-panel-2: #181d29;
    --bbg-line: rgba(255, 255, 255, 0.05);
    --bbg-line-strong: rgba(255, 255, 255, 0.09);
    --bbg-line-soft: rgba(255, 255, 255, 0.025);
    --bbg-text: #e6e9ef;
    --bbg-muted: #7d8597;
    --bbg-muted-soft: #4d5468;
    --bbg-accent: #13b955;
    --bbg-accent-soft: #4fcf7d;
    --bbg-up: #13b955;
    --bbg-down: #ea3943;
    --bbg-info: #5b8dea;
    --bbg-amber: #e8a93b;
    --bbg-radius-sm: 1px;
    --bbg-radius-md: 2px;
    --bbg-radius-lg: 2px;
    --bbg-shadow: 0 1px 0 rgba(0, 0, 0, 0.5);
    --bbg-shadow-soft: 0 1px 0 rgba(0, 0, 0, 0.35);
    --bbg-shadow-glow: none;
    --bbg-focus: 0 0 0 1px rgba(19, 185, 85, 0.55);
    --font-mono: "JetBrains Mono", "IBM Plex Mono", "Fira Code", "Consolas", monospace;
    --font-sans: "Inter", "DM Sans", -apple-system, system-ui, sans-serif;
}
* {
    box-sizing: border-box;
}
html {
    background: var(--bbg-bg);
}
body {
    margin: 0;
    padding-top: 0;
    font-family: var(--font-sans);
    background: var(--bbg-bg);
    color: var(--bbg-text);
    line-height: 1.4;
    font-size: 12px;
    font-variant-numeric: tabular-nums;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-rendering: optimizeLegibility;
}
.v, .table-cell, .level-price, input, .Select-control, .DateInput_input {
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum" 1, "zero" 1;
}
button,
input,
select,
textarea {
    font: inherit;
}
a {
    color: inherit;
}
.app-shell {
    position: relative;
    z-index: 1;
    max-width: 2200px;
    margin: 0 auto;
    padding: 6px 10px 36px;
}
.header-bar,
.toolbar,
.panel,
.info-card,
.idea-card,
.level-item,
.telegram-row,
.alert-row,
.tool-guide,
.table-row,
.position-editor {
    border: 1px solid var(--bbg-line);
    box-shadow: var(--bbg-shadow-soft);
}
.header-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    padding: 6px 12px;
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-panel);
    border: 1px solid var(--bbg-line);
    border-bottom: 1px solid var(--bbg-line-strong);
    box-shadow: var(--bbg-shadow-soft);
    min-height: 38px;
}
.brand {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
}
.brand-dot {
    width: 6px;
    height: 6px;
    border-radius: 0;
    background: var(--bbg-accent);
    flex: 0 0 auto;
}
.brand-title {
    margin: 0;
    font-size: 12px;
    line-height: 1.1;
    font-weight: 700;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--bbg-text);
    font-family: var(--font-mono);
}
.brand-subtitle {
    margin: 2px 0 0;
    font-size: 8px;
    color: var(--bbg-muted-soft);
    letter-spacing: 0.22em;
    text-transform: uppercase;
    font-family: var(--font-sans);
}
.status-pill {
    padding: 3px 8px;
    border-radius: 0;
    border: 1px solid rgba(19, 185, 85, 0.45);
    background: rgba(19, 185, 85, 0.08);
    color: var(--bbg-accent);
    font-size: 8px;
    font-weight: 700;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    white-space: nowrap;
    font-family: var(--font-mono);
}
.toolbar {
    margin-top: 4px;
    padding: 8px 10px;
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-panel);
    border: 1px solid var(--bbg-line);
}
.toolbar-grid {
    display: grid;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    gap: 8px;
    align-items: end;
}
.toolbar-grid > div {
    min-width: 0;
}
.control-label {
    margin-bottom: 3px;
    color: var(--bbg-muted-soft);
    font-size: 8px;
    font-weight: 600;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-family: var(--font-mono);
}
.control-stack {
    display: grid;
    gap: 6px;
    align-content: start;
}
.info-cards {
    margin-top: 4px;
    display: flex;
    gap: 4px;
}
.watchlist-row {
    margin-top: 4px;
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
}
.watchlist-tile {
    flex: 1 1 180px;
    min-width: 180px;
    padding: 6px 10px;
    border: 1px solid var(--bbg-line);
    border-left: 2px solid var(--bbg-accent);
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-panel);
    display: grid;
    grid-template-columns: auto 1fr auto;
    grid-template-rows: auto auto;
    gap: 1px 10px;
    align-items: center;
    cursor: pointer;
    transition: border-color 0.12s ease;
}
.watchlist-tile:hover { border-color: var(--bbg-line-strong); }
.watchlist-tile.active { border-left-color: var(--bbg-amber); background: var(--bbg-panel-2); }
.watchlist-tile.regime-mr { border-left-color: var(--bbg-up); }
.watchlist-tile.regime-tf { border-left-color: var(--bbg-down); }
.watchlist-tile-sym {
    grid-row: 1 / span 2;
    font-family: var(--font-mono);
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 0.05em;
    color: var(--bbg-text);
    padding-right: 6px;
    border-right: 1px solid var(--bbg-line);
}
.watchlist-tile-spot {
    grid-column: 2;
    grid-row: 1;
    font-family: var(--font-mono);
    font-size: 12px;
    color: var(--bbg-text);
    text-align: right;
}
.watchlist-tile-net {
    grid-column: 2;
    grid-row: 2;
    font-family: var(--font-mono);
    font-size: 9px;
    color: var(--bbg-muted);
    text-align: right;
    letter-spacing: 0.06em;
}
.watchlist-tile-regime {
    grid-column: 3;
    grid-row: 1 / span 2;
    font-family: var(--font-mono);
    font-size: 8px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 2px 6px;
    border-radius: 2px;
    font-weight: 700;
}
.watchlist-tile.regime-mr .watchlist-tile-regime { background: rgba(19,185,85,0.15); color: var(--bbg-up); }
.watchlist-tile.regime-tf .watchlist-tile-regime { background: rgba(234,57,67,0.15); color: var(--bbg-down); }
.watchlist-tile-empty { color: var(--bbg-muted); font-family: var(--font-mono); font-size: 10px; padding: 8px; }
/* ── Command palette ── */
.command-palette {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.55);
    z-index: 9000;
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding-top: 12vh;
}
.command-palette-hidden { display: none; }
.command-palette-modal {
    width: min(560px, 92vw);
    background: var(--bbg-panel);
    border: 1px solid var(--bbg-line-strong);
    border-radius: var(--bbg-radius-md);
    box-shadow: 0 12px 40px rgba(0,0,0,0.6);
    overflow: hidden;
}
.command-palette-head {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border-bottom: 1px solid var(--bbg-line);
    background: var(--bbg-panel-2);
}
.command-palette-input {
    flex: 1 1 auto;
    background: transparent;
    border: none;
    outline: none;
    color: var(--bbg-text);
    font-family: var(--font-mono);
    font-size: 13px;
    padding: 4px;
}
.command-palette-input::placeholder { color: var(--bbg-muted); }
.command-palette-hint {
    font-family: var(--font-mono);
    font-size: 9px;
    letter-spacing: 0.1em;
    color: var(--bbg-muted);
    border: 1px solid var(--bbg-line);
    padding: 1px 5px;
    border-radius: 2px;
}
.command-palette-list {
    max-height: 50vh;
    overflow-y: auto;
    padding: 4px 0;
}
.command-palette-item {
    padding: 6px 12px;
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--bbg-text);
    cursor: pointer;
    border-left: 2px solid transparent;
}
.command-palette-item:hover,
.command-palette-item.palette-highlight {
    background: rgba(255,255,255,0.04);
    border-left-color: var(--bbg-accent);
}
.command-palette-item.palette-hidden { display: none; }
/* Workspace save/load buttons in toolbar */
.workspace-controls {
    display: flex;
    gap: 4px;
    align-items: center;
    margin-top: 4px;
}
.workspace-controls .action-button {
    padding: 4px 8px;
    font-size: 9px;
    letter-spacing: 0.06em;
}
.info-card {
    flex: 1 1 0;
    min-width: 0;
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 3px;
    padding: 6px 10px;
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-panel);
    border: 1px solid var(--bbg-line);
    transition: border-color 0.12s ease;
    max-height: 56px;
    position: relative;
    overflow: hidden;
}
.info-card:hover {
    border-color: var(--bbg-line-strong);
}
.info-card .k {
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--bbg-muted-soft);
    font-weight: 600;
    line-height: 1;
    font-family: var(--font-mono);
}
.info-card .v {
    font-size: 16px;
    line-height: 1.05;
    font-weight: 600;
    color: var(--bbg-text);
    font-family: var(--font-mono);
    letter-spacing: -0.01em;
    font-variant-numeric: tabular-nums;
}
.flip-source-note {
    margin-top: 1px;
    font-size: 8px;
    color: var(--bbg-muted);
    line-height: 1.2;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
}
.dashboard-grid {
    margin-top: 6px;
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    gap: 4px;
    align-items: start;
}
.panel {
    min-width: 0;
    display: flex;
    flex-direction: column;
    height: 100%;
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-panel);
    overflow: hidden;
    transition: border-color 0.12s ease;
}
.panel:hover {
    border-color: var(--bbg-line-strong);
}
.chart-panel {
    grid-column: span 12;
    box-shadow: var(--bbg-shadow);
    background: var(--bbg-panel);
}
.levels-panel-shell,
.telegram-panel-shell,
.ideas-panel-shell,
.heatmap-panel-shell,
.alerts-panel-shell,
.portfolio-panel-shell,
.ops-panel-shell,
.options-backtest-panel-shell {
    position: relative;
}
.levels-panel-shell {
    grid-column: span 12;
}
.telegram-panel-shell,
.ideas-panel-shell {
    grid-column: span 4;
}
.heatmap-panel-shell {
    grid-column: span 8;
}
.alerts-panel-shell,
.ops-panel-shell {
    grid-column: span 6;
}
.portfolio-panel-shell,
.options-backtest-panel-shell {
    grid-column: span 12;
}
.levels-panel-shell,
.telegram-panel-shell,
.ideas-panel-shell,
.heatmap-panel-shell {
    background: var(--bbg-panel);
}
.levels-panel-shell::before,
.telegram-panel-shell::before,
.ideas-panel-shell::before,
.heatmap-panel-shell::before,
.alerts-panel-shell::before,
.portfolio-panel-shell::before,
.ops-panel-shell::before,
.options-backtest-panel-shell::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 2px;
    bottom: 0;
    pointer-events: none;
}
/* Subtle left-edge color tag per section */
.levels-panel-shell::before { background: rgba(19, 185, 85, 0.55); }
.ideas-panel-shell::before { background: rgba(79, 207, 125, 0.55); }
.heatmap-panel-shell::before { background: rgba(234, 57, 67, 0.55); }
.telegram-panel-shell::before { background: rgba(91, 141, 234, 0.55); }
.alerts-panel-shell::before { background: rgba(232, 169, 59, 0.6); }
.portfolio-panel-shell::before { background: rgba(19, 185, 85, 0.55); }
.ops-panel-shell::before { background: rgba(91, 141, 234, 0.55); }
.options-backtest-panel-shell::before { background: rgba(192, 132, 252, 0.6); }
.options-backtest-panel-shell {
    height: auto;
    overflow: visible;
}
.options-backtest-panel-shell .sidebar-body {
    display: block;
    padding: 12px 16px;
}
.options-backtest-panel-shell .dash-graph {
    width: 100%;
    min-height: 0;
    height: auto;
}
.panel-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    padding: 6px 10px;
    border-bottom: 1px solid var(--bbg-line);
    background: rgba(255, 255, 255, 0.015);
    border-left: none;
    min-height: 30px;
}
.chart-panel .panel-head {
    align-items: center;
    padding: 6px 10px;
    background: rgba(255, 255, 255, 0.02);
}
.panel-head-copy {
    min-width: 0;
    display: grid;
    gap: 3px;
}
.levels-panel-shell .panel-head,
.telegram-panel-shell .panel-head,
.ideas-panel-shell .panel-head,
.heatmap-panel-shell .panel-head,
.alerts-panel-shell .panel-head,
.portfolio-panel-shell .panel-head,
.ops-panel-shell .panel-head {
    padding-bottom: 10px;
}
.panel-title {
    margin: 0;
    font-size: 10px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    font-weight: 700;
    color: var(--bbg-text);
    font-family: var(--font-mono);
}
.chart-panel .panel-title {
    color: var(--bbg-text);
}
.chart-panel .panel-title::before {
    content: "» ";
    color: var(--bbg-accent);
}
.panel-subtitle {
    margin: 0;
    font-size: 10px;
    color: var(--bbg-muted);
    line-height: 1.4;
    font-family: var(--font-sans);
}
.panel-subtitle + .panel-subtitle {
    margin-top: 3px;
}
.sidebar-body {
    flex: 1 1 auto;
    padding: 8px 10px;
    display: grid;
    gap: 6px;
    align-content: start;
}
.spot-head-controls {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    flex-wrap: wrap;
    min-width: min(100%, 820px);
}
.chart-type-field {
    min-width: 200px;
}
.chart-metrics-field {
    min-width: 300px;
}
.spot-head-field {
    min-width: 110px;
}
.spot-head-caption {
    padding: 5px 10px;
    border-radius: var(--bbg-radius-md);
    background: rgba(19, 185, 85, 0.06);
    border: 1px solid rgba(19, 185, 85, 0.12);
    color: var(--bbg-accent-soft);
    font-size: 9px;
    letter-spacing: 0.06em;
    font-family: var(--font-mono);
}
.Select-control,
.DateRangePickerInput,
.SingleDatePickerInput,
.text-input {
    min-height: 36px !important;
    border-radius: var(--bbg-radius-md) !important;
    border: 1px solid rgba(255, 255, 255, 0.06) !important;
    background: rgba(24, 28, 40, 0.9) !important;
    color: var(--bbg-text) !important;
    box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.3) !important;
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
    transition: border-color 0.15s ease, box-shadow 0.15s ease, background 0.15s ease;
}
.Select-control:hover,
.DateRangePickerInput:hover,
.SingleDatePickerInput:hover,
.text-input:hover {
    border-color: rgba(19, 185, 85, 0.2) !important;
    background: rgba(28, 32, 48, 0.95) !important;
}
.Select.is-focused > .Select-control,
.DateRangePickerInput:focus-within,
.SingleDatePickerInput:focus-within,
.text-input:focus {
    border-color: rgba(19, 185, 85, 0.5) !important;
    box-shadow: var(--bbg-focus), inset 0 1px 3px rgba(0, 0, 0, 0.3) !important;
    outline: none !important;
}
.Select-menu-outer {
    background: var(--bbg-bg-alt) !important;
    border: 1px solid rgba(19, 185, 85, 0.15) !important;
    border-radius: var(--bbg-radius-md) !important;
    overflow: hidden;
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.7) !important;
    margin-top: 4px !important;
}
.Select-menu {
    background: var(--bbg-bg-alt) !important;
}
.Select-option,
.VirtualizedSelectOption {
    background: var(--bbg-bg-alt) !important;
    color: var(--bbg-text) !important;
    font-size: 12px !important;
    transition: background 0.1s ease;
}
.VirtualizedSelectFocusedOption {
    background: rgba(19, 185, 85, 0.1) !important;
    color: #ffffff !important;
}
.Select-value-label,
.Select-placeholder,
.Select-input > input {
    color: var(--bbg-text) !important;
}
.Select-placeholder {
    color: var(--bbg-muted) !important;
}
.Select--multi .Select-value {
    background: rgba(19, 185, 85, 0.08) !important;
    border: 1px solid rgba(19, 185, 85, 0.2) !important;
    border-radius: var(--bbg-radius-sm) !important;
    padding: 2px 8px !important;
}
.Select--multi .Select-value-icon,
.Select--multi .Select-value-label {
    color: var(--bbg-accent-soft) !important;
}
.DateRangePickerInput,
.SingleDatePickerInput {
    display: flex;
    align-items: center;
    padding: 0 6px;
}
.DateInput,
.SingleDatePickerInput .DateInput {
    background: transparent !important;
}
.DateInput_input {
    background: transparent !important;
    color: var(--bbg-text) !important;
    border: none !important;
    font-size: 13px !important;
    padding: 10px 12px !important;
    line-height: 20px !important;
}
.DateInput_input::placeholder {
    color: var(--bbg-muted) !important;
}
.DateRangePicker_picker,
.SingleDatePicker_picker,
.DayPicker,
.DayPicker__withBorder,
.DayPicker_focusRegion,
.DayPicker_transitionContainer,
.CalendarMonthGrid,
.CalendarMonth,
.CalendarMonth_table,
.CalendarDay,
.DayPicker_weekHeader,
.CalendarMonth_caption,
.DateInput_fang,
.DateInput_fangShape {
    background: var(--bbg-bg-alt) !important;
    color: var(--bbg-text) !important;
}
.CalendarMonth_caption,
.DayPicker_weekHeader {
    color: var(--bbg-muted) !important;
}
.CalendarDay__default,
.CalendarDay__outside,
.CalendarDay__blocked_out_of_range {
    border: 1px solid rgba(255, 255, 255, 0.05) !important;
    background: var(--bbg-bg-alt) !important;
    color: var(--bbg-text) !important;
}
.CalendarDay__default:hover,
.CalendarDay__outside:hover {
    background: rgba(19, 185, 85, 0.12) !important;
    border-color: rgba(19, 185, 85, 0.24) !important;
    color: #ffffff !important;
}
.CalendarDay__selected,
.CalendarDay__selected:active,
.CalendarDay__selected:hover,
.CalendarDay__selected_span {
    background: rgba(19, 185, 85, 0.2) !important;
    border-color: rgba(19, 185, 85, 0.5) !important;
    color: var(--bbg-accent) !important;
}
.CalendarDay__blocked_calendar,
.CalendarDay__blocked_out_of_range {
    color: var(--bbg-muted-soft) !important;
    background: rgba(255, 255, 255, 0.02) !important;
}
.DayPickerNavigation_button,
.DayPickerNavigation_button__default {
    border: 1px solid var(--bbg-line) !important;
    background: var(--bbg-surface) !important;
    color: var(--bbg-text) !important;
}
.DayPickerKeyboardShortcuts_buttonReset {
    color: var(--bbg-accent) !important;
}
.levels-toolbar {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 10px;
}
.level-filter,
.mode-filter {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}
.level-filter label,
.mode-filter label {
    margin: 0 !important;
}
.level-filter input,
.mode-filter input {
    position: absolute;
    opacity: 0;
    pointer-events: none;
}
.level-filter .label-body,
.mode-filter .label-body {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 30px;
    padding: 0 12px;
    border-radius: var(--bbg-radius-md);
    border: 1px solid var(--bbg-line);
    background: rgba(24, 28, 40, 0.8);
    color: var(--bbg-muted);
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    transition: all 0.15s ease;
    cursor: pointer;
}
.level-filter .label-body:hover,
.mode-filter .label-body:hover {
    border-color: rgba(19, 185, 85, 0.3);
    color: var(--bbg-accent);
    background: rgba(19, 185, 85, 0.04);
}
.level-filter input:checked + .label-body,
.mode-filter input:checked + .label-body {
    background: rgba(19, 185, 85, 0.1);
    border-color: rgba(19, 185, 85, 0.4);
    color: var(--bbg-accent);
    box-shadow: 0 0 10px rgba(19, 185, 85, 0.06);
}
.levels-list {
    display: grid;
    gap: 6px;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}
.level-item {
    min-width: 0;
    width: 100%;
    display: grid;
    gap: 6px;
    padding: 12px 14px;
    border-radius: var(--bbg-radius-md);
    background: rgba(24, 28, 40, 0.8);
    transition: all 0.18s ease;
}
.level-item:hover,
.idea-card:hover,
.alert-row:hover,
.telegram-row:hover,
.table-row:hover,
.tool-guide:hover {
    border-color: var(--bbg-line-strong);
    background: rgba(255, 255, 255, 0.02);
}
.level-item-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px;
}
.level-name {
    color: var(--bbg-muted-soft);
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    font-weight: 700;
}
.level-price {
    font-weight: 700;
    font-size: 15px;
    color: var(--bbg-text);
    line-height: 1.2;
    white-space: nowrap;
    font-family: var(--font-mono);
}
.level-metric {
    font-size: 11px;
    color: var(--bbg-muted);
    line-height: 1.35;
}
.level-pill {
    font-size: 8px;
    border-radius: 10px;
    padding: 3px 8px;
    border: 1px solid transparent;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}
.pill-pos {
    color: #13b955;
    border-color: rgba(19, 185, 85, 0.25);
    background: rgba(19, 185, 85, 0.06);
}
.pill-neg {
    color: #ea3943;
    border-color: rgba(234, 57, 67, 0.25);
    background: rgba(234, 57, 67, 0.06);
}
.pill-neutral {
    color: #5b8dea;
    border-color: rgba(91, 141, 234, 0.25);
    background: rgba(91, 141, 234, 0.06);
}
.pill-abs {
    color: var(--bbg-accent);
    border-color: rgba(19, 185, 85, 0.25);
    background: rgba(19, 185, 85, 0.06);
}
.level-item.tone-pos {
    border-color: rgba(19, 185, 85, 0.12);
    border-left: 3px solid rgba(19, 185, 85, 0.5);
    background: linear-gradient(135deg, rgba(19, 185, 85, 0.03) 0%, var(--bbg-bg-alt) 100%);
}
.level-item.tone-neg {
    border-color: rgba(234, 57, 67, 0.12);
    border-left: 3px solid rgba(234, 57, 67, 0.5);
    background: linear-gradient(135deg, rgba(234, 57, 67, 0.03) 0%, var(--bbg-bg-alt) 100%);
}
.level-item.tone-neutral {
    border-color: rgba(91, 141, 234, 0.1);
    border-left: 3px solid rgba(91, 141, 234, 0.4);
    background: linear-gradient(135deg, rgba(91, 141, 234, 0.03) 0%, var(--bbg-bg-alt) 100%);
}
.level-item.tone-abs {
    border-color: rgba(19, 185, 85, 0.1);
    border-left: 3px solid rgba(19, 185, 85, 0.4);
    background: linear-gradient(135deg, rgba(19, 185, 85, 0.03) 0%, rgba(6, 10, 16, 0.9) 100%);
}
.ideas-panel {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
}
.idea-card {
    display: grid;
    gap: 7px;
    height: 100%;
    padding: 14px;
    border-radius: var(--bbg-radius-md);
    background: var(--bbg-bg-alt);
    border-left: 3px solid rgba(19, 185, 85, 0.2);
    transition: all 0.18s ease;
}
.idea-card:hover {
    border-left-color: var(--bbg-accent);
}
.idea-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px;
    margin: 0;
}
.idea-title {
    font-size: 10px;
    color: var(--bbg-accent);
    text-transform: uppercase;
    letter-spacing: 0.16em;
    font-weight: 700;
}
.idea-score {
    font-size: 10px;
    color: var(--bbg-muted);
    font-family: var(--font-mono);
}
.idea-setup {
    font-size: 13px;
    color: var(--bbg-text);
    font-weight: 700;
    line-height: 1.35;
}
.idea-why,
.idea-legs,
.idea-risk,
.idea-hedge,
.idea-expiry {
    font-size: 11px;
    line-height: 1.4;
}
.idea-why,
.idea-legs {
    color: var(--bbg-muted);
}
.idea-risk {
    color: #ea3943;
}
.idea-hedge {
    color: #00cfff;
}
.idea-expiry {
    color: var(--bbg-accent-soft);
}
.dash-graph {
    flex: 1 1 auto;
    height: 58vh;
    min-height: 460px;
    max-height: 720px;
    width: 100%;
    margin: 0 auto;
}
.tool-graph {
    height: 340px;
    width: 100%;
}
.dash-graph .modebar,
.tool-graph .modebar {
    top: 8px !important;
    right: 8px !important;
    padding: 2px !important;
    border: 1px solid var(--bbg-line) !important;
    border-radius: var(--bbg-radius-sm) !important;
    background: rgba(0, 0, 0, 0.9) !important;
    box-shadow: var(--bbg-shadow-soft);
}
.dash-graph .modebar-group,
.tool-graph .modebar-group {
    background: transparent !important;
}
.dash-graph .modebar-btn path,
.tool-graph .modebar-btn path {
    fill: #7a8fa4 !important;
}
.dash-graph .modebar-btn:hover path,
.tool-graph .modebar-btn:hover path {
    fill: var(--bbg-accent) !important;
}
.tool-guide {
    padding: 12px 14px;
    border-radius: var(--bbg-radius-md);
    background: rgba(24, 28, 40, 0.6);
    border-left: 3px solid rgba(0, 207, 255, 0.2);
    transition: all 0.15s ease;
}
.tool-guide summary {
    cursor: pointer;
    color: var(--bbg-info);
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    font-weight: 700;
}
.tool-guide-list {
    margin: 8px 0 0;
    padding-left: 16px;
    color: var(--bbg-muted);
    font-size: 11px;
    line-height: 1.5;
}
.telegram-grid {
    display: grid;
    gap: 6px;
}
.telegram-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 10px 14px;
    border-radius: var(--bbg-radius-md);
    background: rgba(24, 28, 40, 0.8);
    transition: all 0.15s ease;
}
.telegram-label {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--bbg-muted-soft);
    font-weight: 700;
}
.telegram-value {
    font-size: 11px;
    color: var(--bbg-text);
    font-family: var(--font-mono);
}
.telegram-link {
    font-size: 11px;
    color: var(--bbg-info);
    text-decoration: none;
}
.telegram-link:hover {
    color: #7ba6f7;
    text-decoration: underline;
}
.telegram-note {
    color: var(--bbg-muted);
    font-size: 10px;
    line-height: 1.5;
}
.alert-controls {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    align-items: end;
}
#alert-unacked-only label {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-height: 36px;
    padding: 0 12px;
    border-radius: var(--bbg-radius-sm);
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
    color: var(--bbg-muted);
    cursor: pointer;
    transition: border-color 0.12s ease, color 0.12s ease;
}
#alert-unacked-only label:hover {
    border-color: rgba(19, 185, 85, 0.3);
    color: var(--bbg-text);
}
#alert-unacked-only input {
    accent-color: var(--bbg-accent);
}
.alert-row {
    padding: 12px 14px;
    border-radius: var(--bbg-radius-md);
    background: rgba(24, 28, 40, 0.8);
    margin-bottom: 6px;
    border-left: 3px solid var(--bbg-line);
    transition: all 0.18s ease;
}
.alert-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--bbg-muted-soft);
    font-weight: 700;
}
.alert-msg {
    margin-top: 6px;
    font-size: 12px;
    color: var(--bbg-text);
    line-height: 1.45;
}
.alert-badge {
    padding: 3px 8px;
    border-radius: 10px;
    border: 1px solid transparent;
    font-size: 8px;
    font-weight: 700;
    letter-spacing: 0.1em;
}
.alert-sev-high {
    color: #ea3943;
    border-color: rgba(234, 57, 67, 0.4);
    background: rgba(234, 57, 67, 0.08);
}
.alert-sev-medium {
    color: var(--bbg-accent);
    border-color: rgba(19, 185, 85, 0.4);
    background: rgba(19, 185, 85, 0.08);
}
.alert-sev-low {
    color: #5b8dea;
    border-color: rgba(91, 141, 234, 0.3);
    background: rgba(91, 141, 234, 0.08);
}
.position-editor {
    padding: 14px;
    border-radius: var(--bbg-radius-md);
    background: rgba(24, 28, 40, 0.7);
    border-left: 3px solid rgba(19, 185, 85, 0.15);
}
.position-controls {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(6, minmax(130px, 1fr));
    margin-bottom: 10px;
}
.position-actions {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    margin-bottom: 8px;
}
.position-actions .action-button {
    flex: 0 0 auto;
    min-width: 140px;
}
.portfolio-cards {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
}
.mini-table {
    display: grid;
    gap: 4px;
}
.table-row {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    align-items: center;
    padding: 9px 12px;
    border-radius: var(--bbg-radius-sm);
    background: rgba(24, 28, 40, 0.7);
    border-left: 1px solid transparent;
    transition: all 0.15s ease;
}
.table-row:nth-child(even) {
    background: rgba(28, 32, 48, 0.7);
}
.position-table .table-row {
    grid-template-columns: repeat(7, minmax(0, 1fr));
}
.suite-results-table .table-row {
    grid-template-columns: 1.2fr 1fr 0.8fr 0.8fr 0.9fr 0.9fr 0.9fr 0.8fr;
}
.suite-flow-table .table-row {
    grid-template-columns: 1fr 1.4fr 0.7fr 0.7fr 0.9fr 0.9fr 0.8fr;
}
.table-header {
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.16em;
    color: var(--bbg-muted-soft);
    background: transparent;
    border-color: transparent;
    box-shadow: none;
    padding: 0 2px;
    font-weight: 700;
}
.table-cell {
    font-size: 11px;
    color: var(--bbg-text);
    font-family: var(--font-mono);
}
#levels-panel,
#strategy-panel,
#alerts-panel,
#data-health-panel,
#portfolio-summary,
#portfolio-table,
#positions-table,
#strategy-suite-builder-metrics,
#strategy-suite-builder-scenarios,
#strategy-suite-optimizer-summary,
#strategy-suite-optimizer-table,
#strategy-suite-flow-summary,
#strategy-suite-flow-table {
    display: grid;
    gap: 12px;
}
.strategy-suite-panel-shell {
    position: relative;
    grid-column: span 12;
    background: var(--bbg-panel);
}
.strategy-suite-panel-shell::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: linear-gradient(90deg, #13b955, #1dd066, #6ddc8c);
    pointer-events: none;
    border-radius: var(--bbg-radius-lg) var(--bbg-radius-lg) 0 0;
}
.strategy-suite-panel-shell .panel-head {
    padding-bottom: 9px;
}
.strategy-suite-meta {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}
.strategy-suite-note,
.suite-status,
.suite-summary-note {
    color: var(--bbg-muted);
    font-size: 10px;
    line-height: 1.4;
}
.suite-status {
    min-height: 14px;
}
.suite-tabs-wrap {
    padding: 0 14px 14px;
}
.suite-tabs-wrap,
.suite-tabs-wrap .tab-parent,
.suite-tabs-wrap .tab-container,
.suite-tabs-wrap .tab-content,
.suite-tabs-wrap .tab-content--selected {
    background: transparent !important;
    color: var(--bbg-text) !important;
}
.suite-tabs-wrap .tab-content,
.suite-tabs-wrap .tab-content--selected {
    padding-top: 12px;
}
.suite-tabs {
    border: none !important;
}
.suite-tabs .tab {
    border: none !important;
}
.suite-tab {
    border: 1px solid var(--bbg-line) !important;
    border-radius: var(--bbg-radius-md) !important;
    background: rgba(24, 28, 40, 0.8) !important;
    color: var(--bbg-muted) !important;
    padding: 9px 16px !important;
    margin-right: 5px !important;
    font-size: 10px !important;
    font-weight: 700 !important;
    letter-spacing: 0.14em !important;
    text-transform: uppercase !important;
    font-family: var(--font-mono) !important;
    transition: all 0.18s ease;
}
.suite-tab:hover {
    border-color: rgba(19, 185, 85, 0.35) !important;
    color: var(--bbg-accent) !important;
    background: rgba(19, 185, 85, 0.04) !important;
}
.suite-tab-selected {
    background: rgba(19, 185, 85, 0.12) !important;
    border-color: rgba(19, 185, 85, 0.5) !important;
    color: var(--bbg-accent) !important;
    border-bottom: 2px solid var(--bbg-accent) !important;
}
.strategy-suite-body {
    padding: 14px 0 0;
    display: grid;
    gap: 12px;
}
.strategy-suite-controls,
.suite-subgrid,
.suite-summary-grid {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}
.suite-summary-grid {
    align-items: start;
}
.suite-actions {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    align-items: center;
}
.suite-subpanel {
    padding: 14px;
    border-radius: var(--bbg-radius-md);
    border: 1px solid var(--bbg-line);
    background: rgba(24, 28, 40, 0.7);
    box-shadow: none;
    transition: border-color 0.15s ease;
}
.suite-subpanel:hover {
    border-color: rgba(255, 255, 255, 0.08);
}
.suite-subpanel-title {
    margin: 0 0 12px;
    color: var(--bbg-muted);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-family: var(--font-sans);
    padding-bottom: 8px;
    border-bottom: 1px solid var(--bbg-line-soft);
}
.suite-legs {
    display: grid;
    gap: 8px;
}
.suite-leg-row {
    padding: 10px;
    border-radius: var(--bbg-radius-sm);
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
}
.suite-leg-grid {
    display: grid;
    gap: 8px;
    grid-template-columns: 88px 106px 116px 1fr 1fr 106px;
    align-items: end;
}
.suite-leg-label {
    margin-bottom: 5px;
    color: var(--bbg-muted-soft);
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.18em;
    text-transform: uppercase;
}
.suite-enable {
    display: flex;
    align-items: center;
    min-height: 36px;
    padding: 0 10px;
    border-radius: var(--bbg-radius-sm);
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
}
.suite-enable label {
    margin: 0 !important;
    color: var(--bbg-muted);
    font-size: 11px;
}
.suite-enable input {
    margin-right: 8px;
}
.suite-card-grid {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
}
.suite-stat-card {
    min-height: 56px;
    display: grid;
    align-content: space-between;
    gap: 4px;
    padding: 6px 10px;
    border-radius: var(--bbg-radius-md);
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
    border-left: 2px solid rgba(19, 185, 85, 0.45);
    transition: border-color 0.12s ease;
}
.suite-stat-card:hover {
    border-color: var(--bbg-line-strong);
}
.suite-stat-card .k {
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--bbg-muted-soft);
    font-weight: 600;
    font-family: var(--font-mono);
}
.suite-stat-card .v {
    font-size: 16px;
    line-height: 1.05;
    font-weight: 600;
    color: var(--bbg-text);
    font-family: var(--font-mono);
    font-variant-numeric: tabular-nums;
}
.suite-figure {
    height: 360px;
    width: 100%;
}
.suite-scenario-table .table-row {
    grid-template-columns: repeat(5, minmax(0, 1fr));
}
.suite-empty {
    padding: 14px;
    border-radius: var(--bbg-radius-sm);
    border: 1px dashed rgba(19, 185, 85, 0.15);
    color: var(--bbg-muted);
    background: var(--bbg-bg-alt);
}
.replay-inline-controls {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 4px 6px;
    border-radius: var(--bbg-radius-sm);
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
}
.replay-controls {
    display: grid;
    gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    align-items: end;
}
.action-button {
    min-height: 26px;
    padding: 4px 10px;
    border-radius: var(--bbg-radius-md);
    border: 1px solid rgba(19, 185, 85, 0.45);
    background: rgba(19, 185, 85, 0.08);
    color: var(--bbg-accent);
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    cursor: pointer;
    font-family: var(--font-mono);
    transition: background 0.12s ease, border-color 0.12s ease, color 0.12s ease;
    position: relative;
}
.action-button.secondary {
    border-color: var(--bbg-line-strong);
    background: var(--bbg-bg-alt);
    color: var(--bbg-muted);
}
.action-button:hover:not(:disabled) {
    border-color: var(--bbg-accent);
    background: rgba(19, 185, 85, 0.16);
    color: var(--bbg-text);
}
.action-button.secondary:hover:not(:disabled) {
    border-color: rgba(19, 185, 85, 0.4);
    color: var(--bbg-text);
    background: rgba(255, 255, 255, 0.03);
}
.action-button:focus-visible {
    outline: none;
    box-shadow: var(--bbg-focus);
}
.action-button:disabled {
    opacity: 0.35;
    cursor: not-allowed;
}
.action-button.small {
    min-height: 28px;
    padding: 4px 10px;
    font-size: 9px;
}
.text-input {
    width: 100%;
    padding: 8px 10px;
    font-size: 12px;
    font-family: var(--font-mono);
}
.rc-slider {
    padding: 5px 0;
}
.rc-slider-rail {
    background-color: rgba(255, 255, 255, 0.05);
    height: 3px;
    border-radius: 4px;
}
.rc-slider-track {
    background: var(--bbg-accent);
    height: 3px;
    border-radius: 0;
}
.rc-slider-handle {
    width: 12px;
    height: 12px;
    margin-top: -5px;
    border: 1px solid var(--bbg-accent);
    border-radius: 0;
    background: var(--bbg-bg);
    box-shadow: none;
    transition: border-color 0.12s ease;
}
.rc-slider-handle:hover {
    border-color: var(--bbg-accent-soft);
}
.rc-slider-handle:active,
.rc-slider-handle:focus {
    box-shadow: var(--bbg-focus), 0 0 12px rgba(19, 185, 85, 0.3);
    border-color: var(--bbg-accent-soft);
}
.rc-slider-mark-text {
    color: var(--bbg-muted-soft);
    font-size: 9px;
    font-family: var(--font-mono);
}
.section-label {
    grid-column: span 12;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 0 2px;
    color: var(--bbg-muted);
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    user-select: none;
    font-family: var(--font-mono);
}
.section-label::before {
    content: "■";
    color: var(--bbg-accent);
    font-size: 7px;
    line-height: 1;
    flex-shrink: 0;
}
.section-label::after {
    content: "";
    flex: 1;
    height: 1px;
    background: var(--bbg-line);
}
.panel-collapse-btn {
    flex: 0 0 auto;
    background: transparent;
    border: 1px solid var(--bbg-line);
    border-radius: var(--bbg-radius-md);
    color: var(--bbg-muted-soft);
    font-size: 10px;
    width: 26px;
    height: 26px;
    padding: 0;
    cursor: pointer;
    transition: all 0.18s ease;
    display: flex;
    align-items: center;
    justify-content: center;
    line-height: 1;
}
.panel-collapse-btn:hover {
    color: var(--bbg-accent);
    border-color: rgba(19, 185, 85, 0.35);
    background: rgba(19, 185, 85, 0.05);
    transform: scale(1.1);
}
.panel.panel-collapsed .sidebar-body,
.panel.panel-collapsed .suite-tabs-wrap {
    display: none;
}
.panel.panel-collapsed {
    height: auto !important;
    grid-row: span 1 !important;
}
.info-card-spark {
    margin-top: 2px;
    display: flex;
    align-items: flex-end;
    height: 20px;
    max-height: 20px;
    overflow: hidden;
}
/* Scrollbar styling */
::-webkit-scrollbar {
    width: 5px;
    height: 5px;
}
::-webkit-scrollbar-track {
    background: transparent;
}
::-webkit-scrollbar-thumb {
    background: rgba(255, 255, 255, 0.08);
    border-radius: 10px;
}
::-webkit-scrollbar-thumb:hover {
    background: rgba(19, 185, 85, 0.25);
}
@media (max-width: 1480px) {
    .toolbar-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .info-cards {
        flex-wrap: wrap;
    }
    .dashboard-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .page-group {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .chart-panel {
        grid-column: span 2;
    }
    .telegram-panel-shell,
    .ideas-panel-shell {
        grid-column: span 1;
    }
    .levels-panel-shell,
    .heatmap-panel-shell,
    .alerts-panel-shell,
    .ops-panel-shell,
    .portfolio-panel-shell,
    .options-backtest-panel-shell {
        grid-column: span 2;
    }
}
@media (max-width: 980px) {
    .app-shell {
        padding: 12px 12px 20px;
    }
    .header-bar {
        padding: 10px 12px;
    }
    .toolbar {
        padding: 10px 12px;
    }
    .toolbar-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .info-cards {
        flex-wrap: wrap;
    }
    .suite-leg-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .panel-head {
        padding: 10px 12px 8px;
    }
    .sidebar-body {
        padding: 12px;
    }
    .spot-head-controls {
        justify-content: flex-start;
        min-width: 100%;
    }
}
@media (max-width: 760px) {
    .app-shell {
        padding: 8px 8px 16px;
    }
    .header-bar {
        flex-direction: column;
        align-items: flex-start;
    }
    .toolbar-grid,
    .dashboard-grid,
    .page-group,
    .info-cards,
    .alert-controls,
    .position-controls,
    .portfolio-cards,
    .replay-controls,
    .strategy-suite-controls,
    .suite-subgrid,
    .suite-summary-grid,
    .suite-leg-grid {
        grid-template-columns: 1fr;
    }
    .dashboard-grid,
    .page-group {
        gap: 8px;
    }
    .chart-panel,
    .levels-panel-shell,
    .telegram-panel-shell,
    .ideas-panel-shell,
    .heatmap-panel-shell,
    .alerts-panel-shell,
    .portfolio-panel-shell,
    .ops-panel-shell,
    .strategy-suite-panel-shell,
    .options-backtest-panel-shell {
        grid-column: span 1;
        grid-row: span 1;
    }
    .backtest-controls {
        grid-template-columns: 1fr 1fr !important;
    }
    .chart-type-field,
    .chart-metrics-field,
    .spot-head-field {
        min-width: 100%;
    }
    .dash-graph {
        height: 64vh;
        min-height: 480px;
    }
    .table-row,
    .position-table .table-row {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .suite-results-table .table-row,
    .suite-flow-table .table-row,
    .suite-scenario-table .table-row {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .floating-toolbar {
        flex-wrap: wrap;
    }
    .tb-label { display: none; }
    .tb-key   { display: none; }
    .tb-btn   { padding: 0 8px; }
}

/* ── Page navigation ── */
.page-group {
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    gap: 10px;
    align-items: start;
}
/* ── User manual layout ── */
.manual-panel {
    grid-column: span 12;
}
.manual-body {
    padding: 14px 18px 18px 18px;
    display: block;
    column-count: 2;
    column-gap: 24px;
    column-rule: 1px solid var(--bbg-line);
}
@media (min-width: 1600px) {
    .manual-body { column-count: 3; }
}
@media (max-width: 1100px) {
    .manual-body { column-count: 1; }
}
.manual-section {
    break-inside: avoid;
    page-break-inside: avoid;
    margin: 0 0 18px 0;
    padding: 10px 12px;
    border: 1px solid var(--bbg-line);
    border-radius: var(--bbg-radius-md);
    background: rgba(255,255,255,0.015);
    display: block;
}
.manual-heading {
    font-family: var(--font-mono);
    font-size: 11px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--bbg-accent);
    margin: 0 0 8px 0;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--bbg-line);
}
.manual-badge {
    display: inline-block;
    min-width: 16px;
    padding: 1px 5px;
    margin-right: 6px;
    background: var(--bbg-accent);
    color: #000;
    font-weight: 700;
    border-radius: 2px;
    text-align: center;
}
.manual-subsection {
    margin: 10px 0 0 0;
    padding: 6px 0 0 0;
    border-top: 1px dashed var(--bbg-line);
}
.manual-subsection:first-of-type {
    border-top: none;
    padding-top: 0;
    margin-top: 4px;
}
.manual-subheading {
    font-family: var(--font-mono);
    font-size: 10.5px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: var(--bbg-text);
    margin-bottom: 4px;
}
.manual-text {
    font-size: 11px;
    line-height: 1.55;
    color: var(--bbg-text-muted);
    margin: 4px 0;
}
.manual-list {
    margin: 4px 0 4px 16px;
    padding: 0;
    font-size: 11px;
    line-height: 1.55;
    color: var(--bbg-text-muted);
}
.manual-list li { margin: 2px 0; }
.manual-keys-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 6px;
    margin: 8px 0 4px 0;
}
.manual-key-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 5px 8px;
    border: 1px solid var(--bbg-line);
    border-radius: var(--bbg-radius-sm, 2px);
    background: rgba(0,0,0,0.25);
}
.manual-key {
    display: inline-block;
    min-width: 18px;
    padding: 1px 5px;
    background: var(--bbg-accent);
    color: #000;
    font-family: var(--font-mono);
    font-weight: 700;
    font-size: 10px;
    text-align: center;
    border-radius: 2px;
}
.manual-key-desc {
    font-family: var(--font-mono);
    font-size: 10px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--bbg-text);
}
.manual-levels-grid {
    display: grid;
    gap: 4px;
    margin-top: 4px;
}
.manual-level-row {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 3px 0;
}
.manual-level-code {
    flex: 0 0 auto;
    min-width: 34px;
    padding: 1px 5px;
    font-family: var(--font-mono);
    font-size: 9.5px;
    font-weight: 700;
    text-align: center;
    border-radius: 2px;
    background: rgba(255,255,255,0.06);
    color: var(--bbg-text);
    border: 1px solid var(--bbg-line);
}
.manual-level-code.sp { color: var(--bbg-accent); border-color: var(--bbg-accent); }
.manual-level-code.mp { color: #ffb84d; border-color: #ffb84d; }
.manual-level-code.gf { color: #c792ea; border-color: #c792ea; }
.manual-level-code.pos { color: #13b955; border-color: #13b955; }
.manual-level-code.neg { color: #ff4d4d; border-color: #ff4d4d; }
.manual-level-code.abs { color: #4db8ff; border-color: #4db8ff; }
.manual-formula-box {
    margin: 8px 0;
    padding: 8px 10px;
    border: 1px solid var(--bbg-line);
    border-left: 2px solid var(--bbg-accent);
    border-radius: var(--bbg-radius-sm, 2px);
    background: rgba(0,0,0,0.3);
}
.manual-formula {
    font-family: var(--font-mono);
    font-size: 12px;
    color: var(--bbg-accent);
    margin-bottom: 4px;
}
.manual-formula-label {
    font-family: var(--font-mono);
    font-size: 10px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--bbg-text);
    margin-bottom: 4px;
}
.manual-code {
    display: block;
    margin: 6px 0;
    padding: 6px 8px;
    font-family: var(--font-mono);
    font-size: 10.5px;
    color: var(--bbg-accent);
    background: rgba(0,0,0,0.4);
    border: 1px solid var(--bbg-line);
    border-radius: 2px;
    white-space: pre-wrap;
}
.page-group.page-hidden {
    display: none !important;
}
.page-group.page-active {
    /* No animation: instant page swap matches a quant terminal feel */
}

/* ── Top Tab Nav (replaces old floating bottom pill) ── */
.floating-toolbar {
    margin-top: 4px;
    display: flex;
    align-items: stretch;
    gap: 0;
    background: var(--bbg-panel);
    border: 1px solid var(--bbg-line);
    border-radius: var(--bbg-radius-md);
    padding: 0;
    overflow: hidden;
    min-height: 30px;
}
.toolbar-sep {
    width: 1px;
    background: var(--bbg-line);
    flex-shrink: 0;
}
.tb-btn {
    display: flex;
    align-items: center;
    gap: 6px;
    height: 28px;
    border: none;
    border-radius: 0;
    border-bottom: 2px solid transparent;
    background: transparent;
    color: var(--bbg-muted);
    cursor: pointer;
    transition: background 0.1s ease, color 0.1s ease, border-color 0.1s ease;
    padding: 0 14px;
    position: relative;
    line-height: 1;
    font-family: var(--font-mono);
}
.tb-icon {
    font-size: 11px;
    font-family: "Segoe UI Symbol", "Apple Symbols", sans-serif;
    flex-shrink: 0;
    color: var(--bbg-muted-soft);
}
.tb-label {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    font-family: var(--font-mono);
    white-space: nowrap;
}
.tb-key {
    font-size: 8px;
    font-family: var(--font-mono);
    color: var(--bbg-muted-soft);
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid var(--bbg-line);
    border-radius: 1px;
    padding: 1px 4px;
    line-height: 1;
    flex-shrink: 0;
}
.tb-btn:hover {
    background: rgba(255, 255, 255, 0.03);
    color: var(--bbg-text);
}
.tb-btn:hover .tb-icon {
    color: var(--bbg-accent);
}
.tb-btn.active {
    background: rgba(19, 185, 85, 0.08);
    color: var(--bbg-accent);
    border-bottom-color: var(--bbg-accent);
}
.tb-btn.active .tb-icon {
    color: var(--bbg-accent);
}
.tb-btn.active .tb-key {
    color: var(--bbg-accent);
    border-color: rgba(19, 185, 85, 0.35);
}
@media (max-width: 980px) {
    .tb-btn { padding: 0 8px; }
    .tb-label { display: none; }
}

/* ── Dealer Hedge Flow panel ── */
.dealer-flow-summary {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 6px;
    margin-bottom: 6px;
}
.dealer-flow-stat {
    padding: 6px 10px;
    background: var(--bbg-bg-alt);
    border: 1px solid var(--bbg-line);
    border-radius: var(--bbg-radius-md);
    border-left: 2px solid rgba(19, 185, 85, 0.45);
    display: grid;
    gap: 3px;
}
.dealer-flow-stat .k {
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--bbg-muted-soft);
    font-weight: 600;
    font-family: var(--font-mono);
}
.dealer-flow-stat .v {
    font-size: 14px;
    font-weight: 700;
    font-family: var(--font-mono);
    color: var(--bbg-text);
    font-variant-numeric: tabular-nums;
}
.dealer-flow-stat .v.up { color: var(--bbg-up); }
.dealer-flow-stat .v.down { color: var(--bbg-down); }
.dealer-flow-regime {
    padding: 2px 6px !important;
    border: 1px solid currentColor;
    border-radius: 1px;
    font-size: 10px !important;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    display: inline-block;
    width: fit-content;
}
.dealer-flow-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 4px;
}
.dealer-flow-card {
    padding: 8px 10px;
    border: 1px solid var(--bbg-line);
    background: var(--bbg-bg-alt);
    border-radius: var(--bbg-radius-md);
    border-left: 2px solid var(--bbg-line-strong);
    display: grid;
    gap: 4px;
    transition: border-color 0.12s ease;
}
.dealer-flow-card.tone-up { border-left-color: rgba(19, 185, 85, 0.6); }
.dealer-flow-card.tone-down { border-left-color: rgba(234, 57, 67, 0.6); }
.dealer-flow-card.tone-neutral { border-left-color: rgba(125, 133, 151, 0.4); }
.dealer-flow-card .k {
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--bbg-muted-soft);
    font-weight: 600;
    font-family: var(--font-mono);
}
.dealer-flow-card .v {
    font-size: 15px;
    font-weight: 700;
    color: var(--bbg-text);
    font-family: var(--font-mono);
    font-variant-numeric: tabular-nums;
    line-height: 1;
}
.dealer-flow-action {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 9px;
    letter-spacing: 0.14em;
    font-weight: 700;
    font-family: var(--font-mono);
}
.dealer-flow-card.tone-up .dealer-flow-action { color: var(--bbg-up); }
.dealer-flow-card.tone-down .dealer-flow-action { color: var(--bbg-down); }
.dealer-flow-card.tone-neutral .dealer-flow-action { color: var(--bbg-muted); }
.dealer-flow-arrow {
    font-size: 12px;
    line-height: 1;
}
.dealer-flow-units {
    font-size: 9px;
    color: var(--bbg-muted);
    font-family: var(--font-mono);
}
.dealer-flow-panel-shell { grid-column: span 6; }
.vanna-charm-panel-shell { grid-column: span 6; }
.intraday-gex-panel-shell { grid-column: span 6; }
.vol-surface-panel-shell { grid-column: span 6; }
.rv-iv-panel-shell { grid-column: span 6; }
.hedge-backtest-panel-shell { grid-column: span 6; }
.dealer-flow-panel-shell::before,
.vanna-charm-panel-shell::before,
.intraday-gex-panel-shell::before,
.vol-surface-panel-shell::before,
.rv-iv-panel-shell::before,
.hedge-backtest-panel-shell::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 2px;
    bottom: 0;
    pointer-events: none;
}
.dealer-flow-panel-shell::before    { background: rgba(232, 169, 59, 0.6); }
.vanna-charm-panel-shell::before    { background: rgba(91, 141, 234, 0.6); }
.intraday-gex-panel-shell::before   { background: rgba(19, 185, 85, 0.6);  }
.vol-surface-panel-shell::before    { background: rgba(199, 146, 234, 0.6); }
.rv-iv-panel-shell::before          { background: rgba(232, 169, 59, 0.6); }
.hedge-backtest-panel-shell::before { background: rgba(91, 141, 234, 0.6); }
@media (max-width: 1480px) {
    .dealer-flow-panel-shell,
    .vanna-charm-panel-shell,
    .intraday-gex-panel-shell,
    .vol-surface-panel-shell,
    .rv-iv-panel-shell,
    .hedge-backtest-panel-shell { grid-column: span 12; }
}
@media (max-width: 760px) {
    .dealer-flow-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .dealer-flow-summary { grid-template-columns: 1fr; }
    .dealer-flow-panel-shell,
    .vanna-charm-panel-shell { grid-column: span 1; }
}

/* ── Footer status bar ── */
.status-bar {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    z-index: 9000;
    display: flex;
    align-items: center;
    gap: 14px;
    height: 22px;
    padding: 0 12px;
    background: var(--bbg-bg-alt);
    border-top: 1px solid var(--bbg-line-strong);
    font-family: var(--font-mono);
    font-size: 10px;
    color: var(--bbg-muted);
    font-variant-numeric: tabular-nums;
}
.status-bar-section {
    display: flex;
    align-items: center;
    gap: 6px;
}
.status-bar-label {
    color: var(--bbg-muted-soft);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 8px;
    font-weight: 700;
}
.status-bar-value {
    color: var(--bbg-text);
    font-weight: 600;
}
.status-bar-value.up { color: var(--bbg-up); }
.status-bar-value.down { color: var(--bbg-down); }
.status-bar-spacer { flex: 1; }
.status-bar-dot {
    width: 6px;
    height: 6px;
    background: var(--bbg-accent);
    border-radius: 50%;
    flex-shrink: 0;
}
.status-bar-dot.stale { background: var(--bbg-amber); }
.status-bar-dot.dead { background: var(--bbg-down); }
.status-bar-sep {
    width: 1px;
    height: 12px;
    background: var(--bbg-line-strong);
}

"""
PLOT_PAPER_BG = "rgba(0, 0, 0, 0)"
PLOT_PANEL_BG = "rgba(30, 34, 53, 0.95)"
PLOT_FONT_COLOR = "#eaecf0"
PLOT_MUTED = "#8a92a6"
PLOT_GRID_COLOR = "rgba(255, 255, 255, 0.04)"
PLOT_ZERO_COLOR = "rgba(255, 255, 255, 0.10)"
PLOT_BORDER_COLOR = "rgba(255, 255, 255, 0.06)"
PLOT_HOVER_BG = "#1c2030"
PLOT_HOVER_BORDER = "rgba(19, 185, 85, 0.5)"
PLOT_UP_COLOR = "#13b955"
PLOT_DOWN_COLOR = "#ea3943"
PLOT_ACCENT = "#13b955"
PLOT_ACCENT_SOFT = "#6ddc8c"
PLOT_INFO = "#5b8dea"

# ---------------------------------------------------------------------------
# Bokeh helpers
# ---------------------------------------------------------------------------
_BK_FONT = "Inter, DM Sans, system-ui, sans-serif"
_BK_MONO = "IBM Plex Mono, JetBrains Mono, monospace"

_BK_IFRAME_CSS = """
html, body { margin:0; padding:0; background:#141720; overflow:hidden; width:100%; height:100%; }
.bk-root, .bk-Column { width:100% !important; height:100% !important; }
.bk-canvas-wrapper, .bk { background:#141720; }
"""


def _bk_html(p):
    """Serialize a Bokeh figure to an HTML string for html.Iframe srcDoc."""
    html_str = file_html(p, CDN, "")
    # Inject dark background + layout CSS right after <head>
    html_str = html_str.replace(
        "<head>",
        f"<head><meta charset='utf-8'><style>{_BK_IFRAME_CSS}</style>",
        1,
    )
    return html_str


def _style_axis(ax):
    ax.axis_label_text_color = "#8a92a6"
    ax.axis_label_text_font = _BK_FONT
    ax.axis_label_text_font_size = "10px"
    ax.major_label_text_color = "#8a92a6"
    ax.major_label_text_font = _BK_MONO
    ax.major_label_text_font_size = "9px"
    ax.axis_line_color = "#ffffff"
    ax.axis_line_alpha = 0.06
    ax.major_tick_line_color = "#ffffff"
    ax.major_tick_line_alpha = 0.06
    ax.minor_tick_line_color = None


def _bk_theme(p):
    """Apply the Deribit dark theme to a Bokeh figure (all axes + legend)."""
    p.background_fill_color = "#1a1e2e"
    p.border_fill_color = "#141720"
    p.outline_line_color = "#ffffff"
    p.outline_line_alpha = 0.04
    p.min_border_left = 10
    p.min_border_right = 10
    p.min_border_top = 6
    p.min_border_bottom = 6
    # Style all axes (primary + any extra)
    for ax in p.axis:
        _style_axis(ax)
    p.xgrid.grid_line_color = "#ffffff"
    p.xgrid.grid_line_alpha = 0.03
    p.xgrid.grid_line_dash = [2, 4]
    p.ygrid.grid_line_color = "#ffffff"
    p.ygrid.grid_line_alpha = 0.03
    p.ygrid.grid_line_dash = [2, 4]
    for leg in p.legend:
        leg.background_fill_color = "#1c2030"
        leg.background_fill_alpha = 0.92
        leg.border_line_color = "#ffffff"
        leg.border_line_alpha = 0.06
        leg.label_text_color = "#8a92a6"
        leg.label_text_font = _BK_FONT
        leg.label_text_font_size = "9px"
        leg.spacing = 3
        leg.padding = 5
    if p.toolbar:
        p.toolbar.logo = None
    return p


# Iframe sizes match the Bokeh figure heights exactly to avoid scroll bars
_IFRAME_STYLE      = {"width": "100%", "height": "320px", "border": "none", "display": "block"}
_IFRAME_STYLE_TALL = {"width": "100%", "height": "500px", "border": "none", "display": "block"}
_IFRAME_STYLE_HM   = {"width": "100%", "height": "480px", "border": "none", "display": "block"}

SPOT_REFRESH_MS = 5000
ALERT_REFRESH_MS = 30000
HEALTH_REFRESH_MS = 60000
REPLAY_DEFAULT_DAYS = 7
DEFAULT_POSITION_EXPIRY_DAYS = 30
DEFAULT_POSITION_EXPIRY = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=DEFAULT_POSITION_EXPIRY_DAYS)).date().isoformat()
STRATEGY_SUITE_SAVE_PATH = DATA_DIR / "strategy_suite_saved.json"


def resolve_replay_window(start_date, end_date):
    now = pd.Timestamp.now(tz="UTC")
    if start_date:
        start_ts = pd.to_datetime(start_date).tz_localize("UTC")
    else:
        start_ts = now - pd.Timedelta(days=REPLAY_DEFAULT_DAYS)
    if end_date:
        end_ts = pd.to_datetime(end_date).tz_localize("UTC") + pd.Timedelta(days=1)
    else:
        end_ts = now
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    return start_ts, end_ts
VOL_REGIME_CACHE_MINUTES = 10
SPOT_HISTORY_LIMIT = 500
DERIBIT_API_URL = "https://www.deribit.com/api/v2/public/get_tradingview_chart_data"
DERIBIT_RESOLUTION = "60"
DERIBIT_LOOKBACK_MINUTES = 360
DERIBIT_TIMEFRAME_OPTIONS = [
    {"label": "1m", "value": "1"},
    {"label": "5m", "value": "5"},
    {"label": "15m", "value": "15"},
    {"label": "1h", "value": "60"},
    {"label": "4h", "value": "240"},
    {"label": "1d", "value": "1D"},
]
DERIBIT_LOOKBACK_BY_RESOLUTION = {
    "1": 12 * 60,
    "5": 24 * 60,
    "15": 3 * 24 * 60,
    "60": 14 * 24 * 60,
    "240": 60 * 24 * 60,
    "1D": 365 * 24 * 60,
}
DERIBIT_RESOLUTION_TO_MINUTES = {
    "1": 1,
    "5": 5,
    "15": 15,
    "60": 60,
    "240": 240,
    "1D": 1440,
}
TRADINGVIEW_DRAW_TOOLS = [
    "drawline",
    "drawopenpath",
    "drawclosedpath",
    "drawcircle",
    "drawrect",
    "eraseshape",
]
MAIN_CHART_CONFIG = {
    "displaylogo": False,
    "scrollZoom": True,
    "doubleClick": "reset+autosize",
    "responsive": True,
    "editable": True,
    "edits": {
        "shapePosition": True,
        "annotationPosition": True,
    },
    "modeBarButtonsToAdd": TRADINGVIEW_DRAW_TOOLS,
    "modeBarButtonsToRemove": ["lasso2d", "select2d", "toggleSpikelines"],
}
TOOL_CHART_CONFIG = {
    "displaylogo": False,
    "responsive": True,
    "displayModeBar": False,
    "scrollZoom": False,
}
SESSION_BAR_OPTIONS = [
    {"label": "100 bars", "value": 100},
    {"label": "200 bars", "value": 200},
    {"label": "400 bars", "value": 400},
    {"label": "800 bars", "value": 800},
    {"label": "All", "value": "all"},
]
DEFAULT_SESSION_BARS = 400
DEFAULT_GEX_METRICS = ["net_gex", "ag"]
GEX_METRIC_OPTIONS = [
    {"label": "Net GEX", "value": "net_gex"},
    {"label": "AG", "value": "ag"},
    {"label": "Call GEX", "value": "call_gex"},
    {"label": "Put GEX", "value": "put_gex"},
    {"label": "Call OI (no data)", "value": "call_oi", "disabled": True},
    {"label": "Put OI (no data)", "value": "put_oi", "disabled": True},
    {"label": "Call Volume (no data)", "value": "call_volume", "disabled": True},
    {"label": "Put Volume (no data)", "value": "put_volume", "disabled": True},
    {"label": "Power Zone (no data)", "value": "power_zone", "disabled": True},
]
GEX_METRIC_LABELS = {item["value"]: item["label"].replace(" (no data)", "") for item in GEX_METRIC_OPTIONS}
NET_GEX_SCALE = 1_000.0
AG_SCALE = 1_000_000.0
EXCHANGE_ORDER = ["Deribit", "Bybit", "Binance", "OKX"]
EXCHANGE_FILTER_OPTIONS = [{"label": exchange, "value": exchange} for exchange in EXCHANGE_ORDER]
EXCHANGE_COLORS = {
    "Deribit": "#ff9b26",
    "Bybit": "#57b8ff",
    "Binance": "#ffd166",
    "OKX": "#6df2b8",
}
EXCHANGE_FALLBACK_COLORS = ["#f58a40", "#52d69a", "#9eb6ff", "#e07f7f"]
VOL_REGIME_CACHE = {}
CHAIN_CACHE_TTL_SECONDS = 120
CHAIN_CACHE = TTLCache(ttl_seconds=CHAIN_CACHE_TTL_SECONDS, max_entries=64)
DATA_STALE_MINUTES = 20
MIN_STRATEGY_ROWS = 20
MIN_STRATEGY_STRIKES = 8
NET_GEX_STABILITY_WINDOW = 8
NET_GEX_STABILITY_MIN_POINTS = 4
NET_GEX_STABILITY_CV_THRESHOLD = 0.35
NET_GEX_HISTORY = {}


def canonical_exchange_name(raw_value):
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


def ordered_exchange_list(values):
    seen = []
    for item in values:
        normalized = canonical_exchange_name(item)
        if normalized and normalized not in seen:
            seen.append(normalized)
    priority = {name: idx for idx, name in enumerate(EXCHANGE_ORDER)}
    return sorted(seen, key=lambda name: (priority.get(name, len(priority)), name))


def normalize_exchange_selection(selected_values, data):
    if data is None or data.empty:
        available = EXCHANGE_ORDER.copy()
    else:
        available = ordered_exchange_list(data["exchange"].dropna().tolist())
        if not available:
            available = EXCHANGE_ORDER.copy()
    if not selected_values:
        return available
    if isinstance(selected_values, str):
        selected_values = [selected_values]
    selected = []
    for raw in selected_values:
        normalized = canonical_exchange_name(raw)
        if normalized in available and normalized not in selected:
            selected.append(normalized)
    return selected or available


def exchange_color(exchange_name, index=0):
    if exchange_name in EXCHANGE_COLORS:
        return EXCHANGE_COLORS[exchange_name]
    return EXCHANGE_FALLBACK_COLORS[index % len(EXCHANGE_FALLBACK_COLORS)]


def load_data():
    df = pd.read_csv(OPTIONS_FILE)
    df["expiry"] = pd.to_datetime(df["expiry"])
    if "exchange" not in df.columns:
        df["exchange"] = "Deribit"
    df["exchange"] = df["exchange"].map(canonical_exchange_name).fillna("Deribit")
    if "call_oi" not in df.columns:
        df["call_oi"] = 0.0
    if "put_oi" not in df.columns:
        df["put_oi"] = 0.0
    return df


_DATA_CACHE = {"mtime": 0.0, "df": None}


def get_latest_data():
    global df_all
    try:
        current_mtime = OPTIONS_FILE.stat().st_mtime
    except OSError:
        return df_all
    if _DATA_CACHE["df"] is not None and current_mtime == _DATA_CACHE["mtime"]:
        df_all = _DATA_CACHE["df"]
        return df_all
    try:
        df_all = load_data()
        _DATA_CACHE["mtime"] = current_mtime
        _DATA_CACHE["df"] = df_all
    except Exception as exc:
        print(f"[data] load_data failed, serving stale cache: {exc}")
    return df_all


def fmt_price(value):
    return f"{value:,.0f}" if pd.notna(value) else "n/a"


def fmt_metric(value):
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


def nearest_level_above(spot_price, candidates):
    valid = [float(x) for x in candidates if x is not None and pd.notna(x) and float(x) > float(spot_price)]
    if not valid:
        return None
    return min(valid)


def nearest_level_below(spot_price, candidates):
    valid = [float(x) for x in candidates if x is not None and pd.notna(x) and float(x) < float(spot_price)]
    if not valid:
        return None
    return max(valid)


def normalize_strikes(candidates):
    valid = sorted({float(x) for x in candidates if x is not None and pd.notna(x)})
    return valid


def nearest_real_strike(target, strikes):
    if target is None or not pd.notna(target) or not strikes:
        return None
    return min(strikes, key=lambda strike: abs(strike - float(target)))


def next_real_strike_above(target, strikes, steps=1, allow_equal=False):
    if target is None or not pd.notna(target) or not strikes:
        return None
    comp = (lambda s: s >= float(target)) if allow_equal else (lambda s: s > float(target))
    higher = [s for s in strikes if comp(s)]
    if not higher:
        return None
    idx = min(max(int(steps) - 1, 0), len(higher) - 1)
    return higher[idx]


def next_real_strike_below(target, strikes, steps=1, allow_equal=False):
    if target is None or not pd.notna(target) or not strikes:
        return None
    comp = (lambda s: s <= float(target)) if allow_equal else (lambda s: s < float(target))
    lower = [s for s in strikes if comp(s)]
    if not lower:
        return None
    idx = min(max(int(steps) - 1, 0), len(lower) - 1)
    return lower[-(idx + 1)]


def choose_expiry_by_dte(expiry_dates, today_date, min_dte=0, max_dte=30):
    if not expiry_dates:
        return None
    normalized = sorted({pd.Timestamp(x).normalize() for x in expiry_dates if pd.notna(x)})
    if not normalized:
        return None
    candidates = []
    for expiry_date in normalized:
        dte = int((expiry_date - today_date).days)
        if min_dte <= dte <= max_dte:
            candidates.append((abs(dte - ((min_dte + max_dte) / 2.0)), dte, expiry_date))
    if candidates:
        best = sorted(candidates, key=lambda item: (item[0], item[1]))[0][2]
        return best.strftime("%Y-%m-%d")

    later = [d for d in normalized if int((d - today_date).days) > max_dte]
    if later:
        return later[0].strftime("%Y-%m-%d")
    return normalized[-1].strftime("%Y-%m-%d")


def record_net_gex(symbol, net_total):
    history = NET_GEX_HISTORY.setdefault(symbol, [])
    history.append(float(net_total))
    if len(history) > NET_GEX_STABILITY_WINDOW:
        NET_GEX_HISTORY[symbol] = history[-NET_GEX_STABILITY_WINDOW:]


def get_stability_status(symbol):
    values = NET_GEX_HISTORY.get(symbol, [])
    if len(values) < NET_GEX_STABILITY_MIN_POINTS:
        return {"ok": False, "label": "Warming", "detail": f"{len(values)}/{NET_GEX_STABILITY_MIN_POINTS} stability points"}
    series = pd.Series(values, dtype=float)
    mean_abs = max(series.abs().mean(), 1.0)
    cv = float(series.std(ddof=0) / mean_abs)
    stable = cv <= NET_GEX_STABILITY_CV_THRESHOLD
    label = "Stable" if stable else "Unstable"
    return {"ok": stable, "label": label, "detail": f"CV={cv:.2f}"}


def get_data_quality_status(filtered_df):
    reasons = []
    stale_minutes = None
    try:
        stale_minutes = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(OPTIONS_FILE.stat().st_mtime, unit="s", tz="UTC")).total_seconds() / 60.0
    except Exception:
        reasons.append("mtime unavailable")
    if stale_minutes is not None and stale_minutes > DATA_STALE_MINUTES:
        reasons.append(f"stale {stale_minutes:.1f}m")
    if filtered_df is None or filtered_df.empty:
        reasons.append("no rows")
        return {"ok": False, "reasons": reasons}
    if len(filtered_df) < MIN_STRATEGY_ROWS:
        reasons.append(f"low rows ({len(filtered_df)})")
    if int(filtered_df["strike"].nunique()) < MIN_STRATEGY_STRIKES:
        reasons.append(f"few strikes ({filtered_df['strike'].nunique()})")
    return {"ok": len(reasons) == 0, "reasons": reasons}


def parse_stability_cv(stability_detail):
    if not stability_detail:
        return 0.35
    text = str(stability_detail)
    marker = "CV="
    idx = text.find(marker)
    if idx < 0:
        return 0.35
    try:
        return float(text[idx + len(marker):].strip())
    except ValueError:
        return 0.35


def read_collector_status():
    status_path = Path(DATA_DIR) / "collector_status.json"
    try:
        raw = status_path.read_text(encoding="utf-8")
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return payload
    except OSError:
        return {}
    except json.JSONDecodeError:
        return {}
    return {}


def get_oi_wall(df):
    if df is None or df.empty:
        return None
    if "call_oi" not in df.columns or "put_oi" not in df.columns:
        return None
    work = df.copy()
    work["total_oi"] = pd.to_numeric(work["call_oi"], errors="coerce").fillna(0.0) + pd.to_numeric(work["put_oi"], errors="coerce").fillna(0.0)
    if work["total_oi"].max() <= 0:
        return None
    idx = work["total_oi"].idxmax()
    try:
        return float(work.loc[idx, "strike"])
    except Exception:
        return None


def get_cached_vol_regime(symbol, expiries, spot):
    if not symbol or spot is None:
        return "unknown"
    now = pd.Timestamp.now(tz="UTC")
    cached = VOL_REGIME_CACHE.get(symbol)
    if cached and (now - cached["ts"]).total_seconds() < (VOL_REGIME_CACHE_MINUTES * 60):
        return cached["regime"]
    regime = "unknown"
    try:
        expiries = [str(x) for x in expiries if x]
        if expiries:
            client = DeribitClient()
            iv_term = estimate_term_iv(symbol, float(spot), expiries, client)
            regime = classify_vol_regime(iv_term)
    except Exception:
        regime = "unknown"
    VOL_REGIME_CACHE[symbol] = {"ts": now, "regime": regime}
    return regime


def render_professional_ideas(payload):
    if not payload.get("ok"):
        return html.Div(f"Professional engine unavailable: {payload.get('reason', 'unknown')}", className="panel-subtitle")
    ideas = payload.get("ideas") or []
    if not ideas:
        return html.Div("Professional engine returned no tradable strategies", className="panel-subtitle")

    def condor_calendar_hedge(idea_name, idea_expiry, ticket_legs, fallback_text):
        idea_is_condor = "condor" in str(idea_name or "").lower()
        if not idea_is_condor:
            return fallback_text
        short_put = None
        short_call = None
        def normalize_option_type(raw_value):
            raw = str(raw_value or "").strip().lower()
            if raw in ("call", "c"):
                return "call"
            if raw in ("put", "p"):
                return "put"
            return ""
        for leg in ticket_legs:
            side = str(leg.get("side", "")).lower()
            option_type = normalize_option_type(leg.get("type") or leg.get("option_type") or leg.get("right"))
            strike = leg.get("strike")
            if strike is None or not pd.notna(strike):
                continue
            try:
                strike_val = float(strike)
            except (TypeError, ValueError):
                continue
            if side in ("sell", "short") and option_type == "put":
                short_put = strike_val
            elif side in ("sell", "short") and option_type == "call":
                short_call = strike_val
        # Fallback: infer shorts from available call/put strikes if side labels are missing.
        if short_put is None or short_call is None:
            put_candidates = []
            call_candidates = []
            for leg in ticket_legs:
                option_type = normalize_option_type(leg.get("type") or leg.get("option_type") or leg.get("right"))
                strike = leg.get("strike")
                if strike is None or not pd.notna(strike):
                    continue
                try:
                    strike_val = float(strike)
                except (TypeError, ValueError):
                    continue
                if option_type == "put":
                    put_candidates.append(strike_val)
                elif option_type == "call":
                    call_candidates.append(strike_val)
            if short_put is None and put_candidates:
                short_put = max(put_candidates)
            if short_call is None and call_candidates:
                short_call = min(call_candidates)
        expiry_text = str(idea_expiry) if idea_expiry else "current expiry"
        if short_put is None or short_call is None:
            return f"Event-based hedge: use one-sided or both-sided calendars around condor short wings (sell {expiry_text}, buy next expiry)."
        put_strike = fmt_price(short_put)
        call_strike = fmt_price(short_call)
        return (
            f"Event-based hedge: one-sided calendar on threatened wing at {put_strike}P or {call_strike}C "
            f"(sell {expiry_text}, buy next expiry). If direction is uncertain, use both-sided calendars at "
            f"{put_strike}P and {call_strike}C (sell {expiry_text}, buy next expiry)."
        )

    cards = []
    for idea in ideas:
        rr = idea.get("rr")
        max_profit = idea.get("max_profit")
        max_loss = idea.get("max_loss")
        rr_text = f"RR {rr:.2f}" if rr is not None else "RR n/a"
        pnl_text = (
            f" | MaxP {max_profit:,.0f} MaxL {max_loss:,.0f}"
            if max_profit is not None and max_loss is not None
            else ""
        )
        checks = idea.get("checks") or {}
        risk_ok = checks.get("risk_ok", "False")
        issues = [f"{k}:{v}" for k, v in checks.items() if k != "risk_ok"]
        issue_text = " | ".join(issues) if issues else "risk checks passed"
        ticket = idea.get("ticket") or {}
        qty = ticket.get("quantity", 0)
        ticket_legs = ticket.get("legs") or []
        hedge_text = condor_calendar_hedge(
            idea_name=idea.get("name", ""),
            idea_expiry=idea.get("expiry"),
            ticket_legs=ticket_legs,
            fallback_text=idea.get("hedge", "n/a"),
        )
        leg_parts = []
        for leg in ticket_legs:
            side = str(leg.get("side", "")).upper() or "LEG"
            option_type = str(leg.get("type", "")).lower()
            suffix = "C" if option_type == "call" else "P" if option_type == "put" else ""
            strike = leg.get("strike")
            strike_text = "n/a"
            if strike is not None and pd.notna(strike):
                try:
                    strike_text = fmt_price(float(strike))
                except (TypeError, ValueError):
                    strike_text = "n/a"
            leg_parts.append(f"{side} {strike_text}{suffix}")
        order_legs = " / ".join(leg_parts) if leg_parts else "n/a"
        cards.append(
            html.Div(
                className="idea-card",
                children=[
                    html.Div(
                        className="idea-head",
                        children=[
                            html.Div(idea.get("name", "Strategy"), className="idea-title"),
                            html.Div(f"Conviction: {int(idea.get('conviction', 0))}/100", className="idea-score"),
                        ],
                    ),
                    html.Div(f"{idea.get('name', 'Strategy')} | Qty {qty}", className="idea-setup"),
                    html.Div(f"Order Legs: {order_legs}", className="idea-legs"),
                    html.Div(idea.get("rationale", ""), className="idea-why"),
                    html.Div(f"Suggested Expiry: {idea.get('expiry', 'n/a')}", className="idea-expiry"),
                    html.Div(f"Risk: {rr_text}{pnl_text}", className="idea-risk"),
                    html.Div(f"Hedge Option: {hedge_text}", className="idea-hedge"),
                    html.Div(f"Checks: risk_ok={risk_ok}; {issue_text}", className="panel-subtitle"),
                    html.Button(
                        "Apply to Suite",
                        id={"type": "apply-idea-btn", "index": len(cards)},
                        className="action-button small",
                    ),
                ],
            )
        )
    return html.Div(className="ideas-panel", children=cards)


def build_strategy_ideas(symbol, strategy_inputs, suggested_expiry):
    if not isinstance(strategy_inputs, dict):
        return html.Div("No strategy ideas available", className="panel-subtitle")
    if isinstance(suggested_expiry, dict):
        regime_expiry = suggested_expiry.get("regime") or "n/a"
        directional_expiry = suggested_expiry.get("directional") or regime_expiry
        income_expiry = suggested_expiry.get("income") or directional_expiry
    else:
        expiry_text = suggested_expiry if suggested_expiry else "n/a"
        regime_expiry = expiry_text
        directional_expiry = expiry_text
        income_expiry = expiry_text
    regime_input = strategy_inputs.get("regime")
    directional_input = strategy_inputs.get("directional")
    income_input = strategy_inputs.get("income")

    def derive_legs(payload):
        if payload is None:
            return None
        spot = float(payload["spot_price"])
        chain_strikes = normalize_strikes(payload["available_strikes"])
        if not chain_strikes:
            return None
        atm = nearest_real_strike(spot, chain_strikes)
        if atm is None:
            return None
        raw_pos = payload.get("pos_gamma")
        raw_neg = payload.get("neg_gamma")
        raw_abs = payload.get("a_levels")
        p_levels = [float(x) for x in raw_pos] if raw_pos is not None else []
        n_levels = [float(x) for x in raw_neg] if raw_neg is not None else []
        a_lvls = [float(x) for x in raw_abs] if raw_abs is not None else []
        near_res = nearest_level_above(spot, p_levels + a_lvls)
        near_sup = nearest_level_below(spot, n_levels + a_lvls)
        upside_target = nearest_real_strike(near_res, chain_strikes) if near_res is not None else next_real_strike_above(atm, chain_strikes, steps=2)
        downside_target = nearest_real_strike(near_sup, chain_strikes) if near_sup is not None else next_real_strike_below(atm, chain_strikes, steps=2)
        if upside_target is None or downside_target is None:
            return None
        condor_top = nearest_level_above(spot, p_levels)
        condor_bottom = nearest_level_below(spot, n_levels)
        condor_top = nearest_real_strike(condor_top, chain_strikes) if condor_top is not None else next_real_strike_above(atm, chain_strikes, steps=2)
        condor_bottom = nearest_real_strike(condor_bottom, chain_strikes) if condor_bottom is not None else next_real_strike_below(atm, chain_strikes, steps=2)
        put_long = next_real_strike_below(condor_bottom, chain_strikes, steps=1) if condor_bottom is not None else None
        call_long = next_real_strike_above(condor_top, chain_strikes, steps=1) if condor_top is not None else None
        if condor_top is None or condor_bottom is None or put_long is None or call_long is None:
            return None
        return {"spot": spot, "flip": payload.get("flip_strike"), "net_total": float(payload.get("net_total", 0.0)), "atm": atm, "up": upside_target, "down": downside_target, "put_long": put_long, "condor_bottom": condor_bottom, "condor_top": condor_top, "call_long": call_long}

    regime_legs = derive_legs(regime_input)
    directional_legs = derive_legs(directional_input)
    income_legs = derive_legs(income_input)

    def build_condor_calendar_hedge(legs, short_expiry):
        if not legs:
            return "No hedge available."
        put_strike = fmt_price(legs["condor_bottom"])
        call_strike = fmt_price(legs["condor_top"])
        spot = float(legs["spot"])
        dist_up = abs(float(legs["condor_top"]) - spot)
        dist_down = abs(spot - float(legs["condor_bottom"]))
        if dist_up <= dist_down:
            one_side = f"one-sided call calendar at {call_strike}C"
        else:
            one_side = f"one-sided put calendar at {put_strike}P"
        return (
            f"Event-based hedge: {one_side} (sell {short_expiry}, buy next expiry). "
            f"If event direction is uncertain, use both-sided calendars at {put_strike}P and {call_strike}C "
            f"(sell {short_expiry}, buy next expiry)."
        )

    cards = []

    if regime_legs:
        spot = regime_legs["spot"]
        flip = float(regime_legs["flip"]) if regime_legs["flip"] is not None and pd.notna(regime_legs["flip"]) else None
        distance_to_flip_pct = (abs(spot - flip) / spot * 100.0) if flip is not None and spot > 0 else None
        cards.append(
            html.Div(
                className="idea-card",
                children=[
                    html.Div(className="idea-head", children=[html.Div("Regime Shift Setup", className="idea-title"), html.Div("Conviction: 70/100", className="idea-score")]),
                    html.Div(f"Long Straddle at {symbol} {fmt_price(regime_legs['atm'])}", className="idea-setup"),
                    html.Div(
                        f"Spot is {distance_to_flip_pct:.2f}% from Gamma Flip ({fmt_price(flip)}), where realized volatility expansion risk is elevated." if distance_to_flip_pct is not None else "Gamma flip distance unavailable.",
                        className="idea-why",
                    ),
                    html.Div(f"Suggested Expiry: {regime_expiry}", className="idea-expiry"),
                    html.Div("Risk: Theta decay if price stays pinned; manage by cutting on IV crush.", className="idea-risk"),
                    html.Div("Hedge Option: Convert to a Strangle by selling farther OTM wings after IV expansion.", className="idea-hedge"),
                ],
            )
        )
    else:
        cards.append(html.Div("Regime setup unavailable: missing valid strikes/levels for suggested expiry.", className="panel-subtitle"))

    if directional_legs and directional_legs["net_total"] >= 0:
        cards.append(
            html.Div(
                className="idea-card",
                children=[
                    html.Div(className="idea-head", children=[html.Div("Directional Setup", className="idea-title"), html.Div("Conviction: 68/100", className="idea-score")]),
                    html.Div(
                        f"Bull Call Spread: Buy {fmt_price(directional_legs['atm'])}C / Sell {fmt_price(directional_legs['up'])}C",
                        className="idea-setup",
                    ),
                    html.Div(
                        f"Net GEX is positive ({fmt_metric(directional_legs['net_total'])}), favoring mean-reversion and controlled upside toward resistance near {fmt_price(directional_legs['up'])}.",
                        className="idea-why",
                    ),
                    html.Div(f"Suggested Expiry: {directional_expiry}", className="idea-expiry"),
                    html.Div("Risk: Upside capped at short strike; reduce if spot loses gamma flip.", className="idea-risk"),
                    html.Div(f"Hedge Option: Buy a downside put near {fmt_price(directional_legs['down'])} as crash protection.", className="idea-hedge"),
                ],
            )
        )
    elif directional_legs:
        cards.append(
            html.Div(
                className="idea-card",
                children=[
                    html.Div(className="idea-head", children=[html.Div("Directional Setup", className="idea-title"), html.Div("Conviction: 68/100", className="idea-score")]),
                    html.Div(
                        f"Bear Put Spread: Buy {fmt_price(directional_legs['atm'])}P / Sell {fmt_price(directional_legs['down'])}P",
                        className="idea-setup",
                    ),
                    html.Div(
                        f"Net GEX is negative ({fmt_metric(directional_legs['net_total'])}), which increases downside convexity risk toward support around {fmt_price(directional_legs['down'])}.",
                        className="idea-why",
                    ),
                    html.Div(f"Suggested Expiry: {directional_expiry}", className="idea-expiry"),
                    html.Div("Risk: Decay + rebound risk if dealers re-hedge and pin price higher.", className="idea-risk"),
                    html.Div(f"Hedge Option: Buy an upside call near {fmt_price(directional_legs['up'])} to cap squeeze risk.", className="idea-hedge"),
                ],
            )
        )
    else:
        cards.append(html.Div("Directional setup unavailable: missing valid strikes/levels for suggested expiry.", className="panel-subtitle"))

    if income_legs:
        cards.append(
            html.Div(
                className="idea-card",
                children=[
                    html.Div(className="idea-head", children=[html.Div("Income Setup", className="idea-title"), html.Div("Conviction: 60/100", className="idea-score")]),
                    html.Div(
                        f"Iron Condor: {fmt_price(income_legs['put_long'])}P/{fmt_price(income_legs['condor_bottom'])}P and {fmt_price(income_legs['condor_top'])}C/{fmt_price(income_legs['call_long'])}C",
                        className="idea-setup",
                    ),
                    html.Div(
                        f"Use when spot is trading between support {fmt_price(income_legs['condor_bottom'])} and resistance {fmt_price(income_legs['condor_top'])} and realized vol is compressing.",
                        className="idea-why",
                    ),
                    html.Div(f"Suggested Expiry: {income_expiry}", className="idea-expiry"),
                    html.Div("Risk: Gap move through one wing; always define max loss with long wings.", className="idea-risk"),
                    html.Div(f"Hedge Option: {build_condor_calendar_hedge(income_legs, income_expiry)}", className="idea-hedge"),
                ],
            )
        )
    else:
        cards.append(html.Div("Income setup unavailable: missing valid strikes/levels for suggested expiry.", className="panel-subtitle"))

    return html.Div(className="ideas-panel", children=cards)


def summarize_strategy_bucket(bucket_df):
    if bucket_df is None or bucket_df.empty:
        return None
    working = bucket_df.assign(
        total_gex=bucket_df["call_gex"] + bucket_df["put_gex"],
        abs_gex=bucket_df["call_gex"].abs() + bucket_df["put_gex"].abs(),
    )
    by_strike = working.groupby("strike", as_index=False)[["total_gex", "abs_gex"]].sum()
    pos_gamma = by_strike[by_strike["total_gex"] > 0].nlargest(2, "total_gex")["strike"].values
    neg_gamma = by_strike[by_strike["total_gex"] < 0].nsmallest(2, "total_gex")["strike"].values
    a_levels = by_strike.nlargest(2, "abs_gex")["strike"].values
    return {
        "spot_price": float(working["spot_price"].iloc[-1]),
        "flip_strike": float(gamma_flip_level(by_strike)),
        "net_total": float(by_strike["total_gex"].sum()),
        "pos_gamma": pos_gamma,
        "neg_gamma": neg_gamma,
        "a_levels": a_levels,
        "available_strikes": by_strike["strike"].tolist(),
    }


def build_expiry_strategy_panel(symbol, selected_df):
    """Returns (panel_component, ideas_data_list)."""
    if selected_df is None or selected_df.empty:
        return html.Div("No strategy ideas available for this filter", className="panel-subtitle"), []

    quality = get_data_quality_status(selected_df)
    stability = get_stability_status(symbol)
    stability_cv = parse_stability_cv(stability.get("detail"))
    gate_notes = []
    if quality["reasons"]:
        gate_notes.append("Quality: " + ", ".join(quality["reasons"]))
    if stability["label"]:
        gate_notes.append(f"Stability: {stability['label']} ({stability['detail']})")
    if (not quality["ok"]) or (not stability["ok"]):
        return html.Div(
            className="ideas-panel",
            children=[
                html.Div("Trade Ideas gated: data quality/stability checks not satisfied.", className="panel-subtitle"),
                html.Div(" | ".join(gate_notes), className="panel-subtitle"),
            ],
        ), []

    pro_payload = generate_professional_ideas(
        selected_df,
        symbol=symbol,
        account_equity=ACCOUNT_EQUITY_USD,
        stability_cv=stability_cv,
    )
    backtest = run_walk_forward_backtest(SNAPSHOT_DB, symbol)
    backtest_note = "Backtest: unavailable"
    if backtest.get("ok"):
        result = backtest.get("result", {})
        backtest_note = (
            f"Backtest WF: trades {result.get('trades', 0)} | "
            f"hit {100 * float(result.get('hit_rate', 0.0)):.1f}% | "
            f"exp {100 * float(result.get('expectancy', 0.0)):.2f}% | "
            f"ret {100 * float(result.get('total_return', 0.0)):.2f}% | "
            f"mdd {100 * float(result.get('max_drawdown', 0.0)):.2f}%"
        )
    metrics_hist = load_metric_history(SNAPSHOT_DB, symbol, limit=20)
    hist_note = f"Metrics samples: {len(metrics_hist)}"
    if pro_payload.get("ok"):
        expiry_map = pro_payload.get("expiry_map", {})
        subtitle = html.Div(
            f"DTE-selected expiries: Regime {expiry_map.get('regime', 'n/a')} | "
            f"Directional {expiry_map.get('directional', 'n/a')} | "
            f"Income {expiry_map.get('income', 'n/a')}",
            className="panel-subtitle",
        )
        ideas_data = pro_payload.get("ideas") or []
        return html.Div(
            className="ideas-panel",
            children=[
                subtitle,
                html.Div(" | ".join(gate_notes + [hist_note]), className="panel-subtitle"),
                html.Div(backtest_note, className="panel-subtitle"),
                render_professional_ideas(pro_payload),
            ],
        ), ideas_data

    # Fallback heuristic path
    work = selected_df.copy()
    work["expiry_date"] = pd.to_datetime(work["expiry"]).dt.normalize()
    today_date = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    forward_expiries = sorted([d for d in work["expiry_date"].dropna().unique().tolist() if d >= today_date])
    if not forward_expiries:
        return html.Div("No upcoming expiries available in current selection", className="panel-subtitle"), []
    forward_df = work[work["expiry_date"].isin(forward_expiries)].copy()
    forward_df["expiry_key"] = forward_df["expiry_date"].dt.strftime("%Y-%m-%d")
    strikes_by_expiry = (
        forward_df.groupby("expiry_key")["strike"]
        .apply(lambda values: normalize_strikes(values.tolist()))
        .to_dict()
    )

    fallback_expiry = pd.Timestamp(forward_expiries[0]).strftime("%Y-%m-%d")
    expiry_map = {
        "regime": choose_expiry_by_dte(forward_expiries, today_date, min_dte=5, max_dte=12) or fallback_expiry,
        "directional": choose_expiry_by_dte(forward_expiries, today_date, min_dte=10, max_dte=21) or fallback_expiry,
        "income": choose_expiry_by_dte(forward_expiries, today_date, min_dte=14, max_dte=35) or fallback_expiry,
    }
    strikes_map = {
        "regime": strikes_by_expiry.get(expiry_map["regime"], []),
        "directional": strikes_by_expiry.get(expiry_map["directional"], []),
        "income": strikes_by_expiry.get(expiry_map["income"], []),
    }
    strategy_inputs = {}
    for setup_name in ("regime", "directional", "income"):
        expiry_key = expiry_map[setup_name]
        expiry_df = forward_df[forward_df["expiry_key"] == expiry_key]
        summary = summarize_strategy_bucket(expiry_df)
        if summary is None:
            strategy_inputs[setup_name] = None
        else:
            summary["available_strikes"] = strikes_map[setup_name]
            strategy_inputs[setup_name] = summary

    subtitle = html.Div(
        f"DTE-selected expiries: Regime {expiry_map['regime']} | Directional {expiry_map['directional']} | Income {expiry_map['income']}",
        className="panel-subtitle",
    )
    ideas = build_strategy_ideas(
        symbol,
        strategy_inputs,
        expiry_map,
    )
    return html.Div(
        className="ideas-panel",
        children=[
            subtitle,
            html.Div(" | ".join(gate_notes + [hist_note]), className="panel-subtitle"),
            html.Div(backtest_note, className="panel-subtitle"),
            ideas,
        ],
    ), []


def build_sparkline_graph(prices):
    """Build a minimal Plotly sparkline for the spot price info card."""
    if not prices or len(prices) < 2:
        return None
    vals = [float(p) for p in prices]
    trend_color = "#4ec88c" if vals[-1] >= vals[0] else "#ff7f8d"
    pct_chg = (vals[-1] - vals[0]) / vals[0] * 100.0 if vals[0] != 0 else 0.0
    sign = "+" if pct_chg >= 0 else ""
    fig = go.Figure(
        data=[go.Scatter(
            y=vals,
            mode="lines",
            line=dict(color=trend_color, width=1.5),
            hoverinfo="none",
        )],
        layout=go.Layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=0, b=0, pad=0),
            xaxis=dict(visible=False, fixedrange=True),
            yaxis=dict(visible=False, fixedrange=True),
            showlegend=False,
        ),
    )
    return html.Div(
        style={"display": "flex", "alignItems": "center", "gap": "6px", "marginTop": "4px"},
        children=[
            dcc.Graph(
                figure=fig,
                config={"displayModeBar": False, "staticPlot": True},
                style={"height": "34px", "width": "110px", "margin": "0"},
            ),
            html.Span(
                f"{sign}{pct_chg:.2f}%",
                style={"color": trend_color, "fontSize": "11px", "fontFamily": "JetBrains Mono, IBM Plex Mono, monospace"},
            ),
        ],
    )


def build_empty_figure(message="No data available for current filters"):
    p = bokeh_figure(height=300, sizing_mode="stretch_width", toolbar_location=None)
    _bk_theme(p)
    p.xaxis.visible = False
    p.yaxis.visible = False
    p.xgrid.visible = False
    p.ygrid.visible = False
    lbl = Label(
        x=0.5, y=0.5, x_units="screen", y_units="screen",
        text=message, text_color="#a8b7c7", text_font_size="13px",
        text_align="center", text_baseline="middle",
        background_fill_color="#0d1821", background_fill_alpha=0.8,
        border_line_color="#92aac6", border_line_alpha=0.18,
    )
    p.add_layout(lbl)
    return _bk_html(p)


def fmt_money(value, decimals=0):
    if value is None or not pd.notna(value):
        return "n/a"
    return f"${float(value):,.{int(decimals)}f}"


def fmt_pct(value, decimals=1):
    if value is None or not pd.notna(value):
        return "n/a"
    return f"{float(value) * 100:.{int(decimals)}f}%"


def fmt_bound(value):
    if value is None or not pd.notna(value):
        return "Open"
    return fmt_money(value, 0)


def make_suite_card(label, value, subtext=None):
    children = [html.Div(label, className="k"), html.Div(value, className="v")]
    if subtext:
        children.append(html.Div(subtext, className="panel-subtitle"))
    return html.Div(children, className="suite-stat-card")


def blank_strategy_suite_legs():
    return [
        {
            "row_id": idx,
            "enabled": idx == 1,
            "action": "buy",
            "type": "call",
            "expiry": None,
            "strike": None,
            "quantity": 1.0,
        }
        for idx in range(1, 5)
    ]


def strategy_suite_saved_entries(symbol):
    symbol = str(symbol or "BTC").upper()
    entries = [item for item in load_saved_strategies(STRATEGY_SUITE_SAVE_PATH) if str(item.get("symbol") or "").upper() == symbol]
    return sorted(entries, key=lambda item: (str(item.get("saved_at") or ""), str(item.get("name") or "").lower()), reverse=True)


def strategy_suite_saved_options(symbol):
    options = []
    for item in strategy_suite_saved_entries(symbol):
        label = f"{item.get('name')} · {template_label(item.get('template') or 'custom')}"
        options.append({"label": label, "value": item.get("name")})
    return options


def collect_strategy_suite_legs(enabled_values, actions, leg_types, expiries, strikes, quantities):
    lengths = [len(values or []) for values in (enabled_values, actions, leg_types, expiries, strikes, quantities)]
    row_count = max(lengths + [4])
    legs = []
    for idx in range(row_count):
        enabled_raw = (enabled_values or [])[idx] if idx < len(enabled_values or []) else []
        enabled = enabled_raw is True or (isinstance(enabled_raw, list) and "on" in enabled_raw)
        strike = (strikes or [None])[idx] if idx < len(strikes or []) else None
        quantity = (quantities or [0])[idx] if idx < len(quantities or []) else 0
        strike_num = pd.to_numeric(strike, errors="coerce")
        qty_num = pd.to_numeric(quantity, errors="coerce")
        legs.append(
            {
                "row_id": idx + 1,
                "enabled": bool(enabled),
                "action": (actions or ["buy"])[idx] if idx < len(actions or []) else "buy",
                "type": (leg_types or ["call"])[idx] if idx < len(leg_types or []) else "call",
                "expiry": (expiries or [None])[idx] if idx < len(expiries or []) else None,
                "strike": float(strike_num) if pd.notna(strike_num) else None,
                "quantity": float(qty_num) if pd.notna(qty_num) else 0.0,
            }
        )
    return normalize_builder_legs(legs)


def build_strategy_suite_leg_rows(builder_state, chain_df):
    builder_state = dict(builder_state or {})
    chain_df = chain_df if chain_df is not None else pd.DataFrame()
    expiries = list_expiries(chain_df)
    expiry_options = [{"label": exp, "value": exp} for exp in expiries]
    legs = normalize_builder_legs(builder_state.get("legs") or blank_strategy_suite_legs())
    while len(legs) < 4:
        legs.append(blank_strategy_suite_legs()[len(legs)])
    rows = []
    for idx, leg in enumerate(legs[:4], start=1):
        leg_type = str(leg.get("type") or "call")
        expiry_value = leg.get("expiry") if leg.get("expiry") in expiries else (expiries[0] if expiries and leg_type != "spot" and idx == 1 else leg.get("expiry"))
        strike_list = list_strikes(chain_df, expiry_value, leg_type) if leg_type in {"call", "put"} else []
        strike_options = [{"label": fmt_price(strike), "value": float(strike)} for strike in strike_list]
        strike_value = leg.get("strike")
        if strike_list and strike_value is None and leg_type in {"call", "put"}:
            chain_spot_for_pick = get_chain_spot(chain_df, builder_state.get("symbol") or "BTC")
            if chain_spot_for_pick is not None:
                strike_value = nearest_strike(strike_list, chain_spot_for_pick)
        rows.append(
            html.Div(
                className="suite-leg-row",
                children=[
                    html.Div(f"Leg {idx}", className="suite-subpanel-title"),
                    html.Div(
                        className="suite-leg-grid",
                        children=[
                            html.Div(
                                [
                                    html.Div("Enabled", className="suite-leg-label"),
                                    dcc.Checklist(
                                        id={"type": "suite-leg-enabled", "index": idx},
                                        className="suite-enable",
                                        options=[{"label": "Active", "value": "on"}],
                                        value=["on"] if leg.get("enabled") else [],
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Div("Action", className="suite-leg-label"),
                                    dcc.Dropdown(
                                        id={"type": "suite-leg-action", "index": idx},
                                        options=[
                                            {"label": "Buy", "value": "buy"},
                                            {"label": "Sell", "value": "sell"},
                                        ],
                                        value=leg.get("action") or "buy",
                                        clearable=False,
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Div("Type", className="suite-leg-label"),
                                    dcc.Dropdown(
                                        id={"type": "suite-leg-type", "index": idx},
                                        options=[
                                            {"label": "Call", "value": "call"},
                                            {"label": "Put", "value": "put"},
                                            {"label": "Spot", "value": "spot"},
                                        ],
                                        value=leg_type,
                                        clearable=False,
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Div("Expiry", className="suite-leg-label"),
                                    dcc.Dropdown(
                                        id={"type": "suite-leg-expiry", "index": idx},
                                        options=expiry_options,
                                        value=expiry_value,
                                        clearable=True,
                                        disabled=leg_type == "spot",
                                        placeholder="Pick expiry",
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Div("Strike", className="suite-leg-label"),
                                    dcc.Dropdown(
                                        id={"type": "suite-leg-strike", "index": idx},
                                        options=strike_options,
                                        value=float(strike_value) if strike_value not in (None, "") else None,
                                        clearable=True,
                                        disabled=leg_type == "spot",
                                        placeholder="Pick strike",
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Div("Qty", className="suite-leg-label"),
                                    dcc.Input(
                                        id={"type": "suite-leg-qty", "index": idx},
                                        type="number",
                                        min=0,
                                        step=0.25,
                                        value=leg.get("quantity") or 0,
                                        className="text-input",
                                    ),
                                ]
                            ),
                        ],
                    ),
                ],
            )
        )
    return rows


def build_strategy_suite_summary(report):
    if not report or not report.get("ok"):
        return html.Div(report.get("reason", "Strategy builder waiting for valid legs."), className="suite-summary-note")
    breakevens = report.get("breakevens") or []
    breakeven_text = ", ".join(fmt_price(value) for value in breakevens[:4]) if breakevens else "none"
    note = (
        f"Spot {fmt_money(report.get('spot'), 0)} | Avg IV {fmt_pct(report.get('avg_iv'))} | "
        f"Breakevens {breakeven_text} | Commission {fmt_money(report.get('commission_total'), 0)}"
    )
    return html.Div(note, className="suite-summary-note")


def build_strategy_suite_metrics(report):
    if not report or not report.get("ok"):
        return html.Div("Payoff metrics will appear once every active leg has a valid quote.", className="suite-empty")
    debit_or_credit = (
        ("Net Credit", fmt_money(report.get("net_credit"), 0), None)
        if float(report.get("net_credit") or 0.0) > 0
        else ("Net Debit", fmt_money(report.get("net_cost"), 0), f"Fees {fmt_money(report.get('commission_total'), 0)}")
    )
    greek_now = report.get("net_greeks_now") or {}
    cards = [
        make_suite_card("Spot", fmt_money(report.get("spot"), 0)),
        make_suite_card(debit_or_credit[0], debit_or_credit[1], debit_or_credit[2]),
        make_suite_card("Max Profit", fmt_bound(report.get("max_profit"))),
        make_suite_card("Max Loss", fmt_bound(report.get("max_loss"))),
        make_suite_card("Chance Of Profit", fmt_pct(report.get("probability_of_profit"))),
        make_suite_card("Avg Implied Vol", fmt_pct(report.get("avg_iv"))),
        make_suite_card("Net Delta", f"{float(greek_now.get('delta', 0.0)):+.2f}"),
        make_suite_card("Net Vega", f"{float(greek_now.get('vega', 0.0)):+.2f}"),
    ]
    return html.Div(cards, className="suite-card-grid")


def build_strategy_suite_payoff_figure(report, title_text):
    if not report or not report.get("ok"):
        return build_empty_figure("Strategy payoff will appear once the selected legs can be priced.")
    grid = list(report.get("grid") or [])
    curves = report.get("curves") or {}
    now_y = list(curves.get("now") or [])
    eval_y = list(curves.get("eval") or [])
    expiry_y = list(curves.get("expiry") or [])

    p = bokeh_figure(
        height=320, sizing_mode="stretch_width",
        x_axis_label="Underlying Price", y_axis_label="Profit / Loss (USD)",
        tools="pan,wheel_zoom,box_zoom,reset,crosshair,hover",
        active_scroll="wheel_zoom",
        title=title_text,
    )
    p.title.text_color = "#eaecf0"
    p.title.text_font = _BK_FONT
    p.title.text_font_size = "13px"

    if now_y:
        p.line(grid, now_y, color="#a8b7c7", line_width=2, legend_label="Mark To Market Now")
    if eval_y:
        p.line(grid, eval_y, color="#75c4ff", line_width=2.4,
               legend_label=f"P/L In {float(report.get('eval_days') or 0):.0f}d")
    if expiry_y:
        p.line(grid, expiry_y, color="#f0aa4d", line_width=3, legend_label="At Final Expiry")

    breakevens = report.get("breakevens") or []
    if breakevens:
        be_src = ColumnDataSource(dict(
            x=breakevens, y=[0] * len(breakevens),
            label=[fmt_price(b) for b in breakevens],
        ))
        p.scatter("x", "y", source=be_src, size=10, color="#eaecf0",
                  line_color="#071019", line_width=1.5, legend_label="Breakeven")
        # Price label above each breakeven dot
        p.add_layout(LabelSet(
            x="x", y="y", text="label", source=be_src,
            x_offset=0, y_offset=10,
            text_color="#eaecf0", text_font=_BK_MONO, text_font_size="9px",
            text_align="center",
        ))

    # Zero P&L line with label
    p.add_layout(Span(location=0, dimension="width",
                      line_color="#8a92a6", line_alpha=0.30, line_width=1))
    p.add_layout(Label(
        x=1.0, y=0, x_units="screen", y_units="data", x_offset=-4,
        text="0", text_color="#8a92a6", text_font=_BK_MONO,
        text_font_size="9px", text_align="right", text_baseline="middle",
        background_fill_color="#04090f", background_fill_alpha=0.7,
    ))

    # Spot price reference line
    spot_val = float(report.get("spot") or 0.0)
    if spot_val:
        p.add_layout(Span(location=spot_val, dimension="height",
                          line_color="#75c4ff", line_alpha=0.42, line_width=1, line_dash="dotted"))
        p.add_layout(Label(
            x=spot_val, y=0, x_units="data", y_units="screen",
            y_offset=4, text=f"SP {fmt_price(spot_val)}",
            text_color="#75c4ff", text_font=_BK_MONO, text_font_size="9px",
            text_align="center",
        ))

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [("Spot", "@x{0,0}"), ("P/L", "@y{$0,0}")]
        hover.mode = "vline"

    p.yaxis.formatter = NumeralTickFormatter(format="$0,0")
    p.xaxis.formatter = NumeralTickFormatter(format="0,0")
    p.legend.orientation = "horizontal"
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    _bk_theme(p)
    return _bk_html(p)


def build_strategy_suite_scenarios(report):
    if not report or not report.get("ok"):
        return html.Div("Scenario table will populate once the legs form a valid strategy.", className="suite-empty")
    rows = [
        html.Div(
            className="table-row table-header",
            children=[
                html.Div("Move", className="table-cell"),
                html.Div("Spot", className="table-cell"),
                html.Div("Now", className="table-cell"),
                html.Div(f"{float(report.get('eval_days') or 0):.0f}d", className="table-cell"),
                html.Div("Expiry", className="table-cell"),
            ],
        )
    ]
    for row in report.get("scenario_rows") or []:
        rows.append(
            html.Div(
                className="table-row",
                children=[
                    html.Div(f"{float(row.get('move_pct', 0.0)):+.0f}%", className="table-cell"),
                    html.Div(fmt_money(row.get("spot"), 0), className="table-cell"),
                    html.Div(f"{float(row.get('now_pnl', 0.0)):+,.0f}", className="table-cell"),
                    html.Div(f"{float(row.get('eval_pnl', 0.0)):+,.0f}", className="table-cell"),
                    html.Div(f"{float(row.get('expiry_pnl', 0.0)):+,.0f}", className="table-cell"),
                ],
            )
        )
    return html.Div(rows, className="mini-table suite-scenario-table")


def build_strategy_suite_optimizer_summary(candidates):
    if not candidates:
        return html.Div("Optimizer is ready. Load a live BTC or ETH chain to score strategy candidates.", className="suite-empty")
    best = candidates[0]
    report = best.get("report") or {}
    expiry_text = best.get("primary_expiry") or "n/a"
    if best.get("secondary_expiry"):
        expiry_text = f"{expiry_text} -> {best.get('secondary_expiry')}"
    entry_cash = float(best.get("net_cost") or 0.0) - float(best.get("net_credit") or 0.0)
    cards = [
        make_suite_card("Best Match", best.get("template_label") or "n/a", expiry_text),
        make_suite_card("Chance Of Profit", fmt_pct(best.get("probability_of_profit"))),
        make_suite_card("Entry", fmt_money(entry_cash, 0), f"Score {float(best.get('score') or 0.0):.2f}"),
        make_suite_card("Max Profit", fmt_bound(best.get("max_profit"))),
        make_suite_card("Max Loss", fmt_bound(best.get("max_loss"))),
        make_suite_card("Bias", str(best.get("bias") or "neutral").title(), f"Spot {fmt_money(report.get('spot'), 0)}"),
    ]
    return html.Div(cards, className="suite-card-grid")


def build_strategy_suite_optimizer_figure(candidates):
    if not candidates:
        return build_empty_figure("No optimizer candidates matched the current filters.")
    best = candidates[:12]
    x_values, y_values, sizes, texts, scores, hover_texts = [], [], [], [], [], []
    for item in best:
        entry_cash = float(item.get("net_cost") or 0.0) - float(item.get("net_credit") or 0.0)
        reward = item.get("max_profit")
        reward_size = abs(float(reward)) if reward is not None and pd.notna(reward) else max(float(item.get("net_cost") or 0.0), 1.0)
        x_values.append(entry_cash)
        y_values.append(float(item.get("probability_of_profit") or 0.0) * 100.0)
        sizes.append(max(14.0, min(36.0, 12.0 + (reward_size ** 0.35))))
        texts.append(item.get("template_label") or "Candidate")
        scores.append(float(item.get("score") or 0.0))
        expiry_text = item.get("primary_expiry") or "n/a"
        if item.get("secondary_expiry"):
            expiry_text = f"{expiry_text} -> {item.get('secondary_expiry')}"
        hover_texts.append(f"{item.get('template_label')}\n{expiry_text}\nScore {float(item.get('score') or 0.0):.2f}")

    src = ColumnDataSource(dict(x=x_values, y=y_values, size=sizes, label=texts, score=scores, tip=hover_texts))

    p = bokeh_figure(
        height=360, sizing_mode="stretch_width",
        x_axis_label="Entry Cashflow (debit = positive, credit = negative)",
        y_axis_label="Chance Of Profit (%)",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        title="Optimizer Frontier",
    )
    p.title.text_color = "#eaecf0"
    p.title.text_font = _BK_FONT
    p.title.text_font_size = "13px"

    min_s, max_s = (min(scores), max(scores)) if scores else (0.0, 1.0)
    if min_s == max_s:
        max_s = min_s + 1.0
    mapper = LinearColorMapper(palette=["#75c4ff", "#f0aa4d"], low=min_s, high=max_s)
    p.scatter("x", "y", source=src, size="size",
               fill_color={"field": "score", "transform": mapper},
               line_color="#04090f", line_alpha=0.9, line_width=1, fill_alpha=0.92)

    # Strategy name labels above each bubble (non-overlapping is handled by Bokeh layout)
    labels = LabelSet(
        x="x", y="y", text="label", source=src,
        x_offset=0, y_offset=8,
        text_color="#a8b7c7", text_font=_BK_FONT, text_font_size="9px",
        text_align="center", text_baseline="bottom",
    )
    p.add_layout(labels)

    p.add_layout(Span(location=50, dimension="width",
                      line_color="#8a92a6", line_alpha=0.22, line_width=1, line_dash="dotted"))
    p.add_layout(Span(location=0, dimension="height",
                      line_color="#8a92a6", line_alpha=0.22, line_width=1, line_dash="dotted"))

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Strategy", "@label"),
            ("Entry",    "@x{$0,0}"),
            ("POP",      "@y{0.0}%"),
            ("Details",  "@tip"),
        ]

    p.xaxis.formatter = NumeralTickFormatter(format="$0,0")
    p.yaxis.formatter = NumeralTickFormatter(format="0.0")
    _bk_theme(p)
    return _bk_html(p)


def build_strategy_suite_optimizer_table(candidates):
    if not candidates:
        return html.Div("No candidates yet.", className="suite-empty")
    rows = [
        html.Div(
            className="table-row table-header",
            children=[
                html.Div("Template", className="table-cell"),
                html.Div("Expiry", className="table-cell"),
                html.Div("Bias", className="table-cell"),
                html.Div("POP", className="table-cell"),
                html.Div("Entry", className="table-cell"),
                html.Div("Max Profit", className="table-cell"),
                html.Div("Max Loss", className="table-cell"),
                html.Div("Score", className="table-cell"),
            ],
        )
    ]
    for item in candidates[:10]:
        expiry_text = item.get("primary_expiry") or "n/a"
        if item.get("secondary_expiry"):
            expiry_text = f"{expiry_text} -> {item.get('secondary_expiry')}"
        entry_cash = float(item.get("net_cost") or 0.0) - float(item.get("net_credit") or 0.0)
        rows.append(
            html.Div(
                className="table-row",
                children=[
                    html.Div(str(item.get("template_label") or "n/a"), className="table-cell"),
                    html.Div(expiry_text, className="table-cell"),
                    html.Div(str(item.get("bias") or "neutral").title(), className="table-cell"),
                    html.Div(fmt_pct(item.get("probability_of_profit")), className="table-cell"),
                    html.Div(fmt_money(entry_cash, 0), className="table-cell"),
                    html.Div(fmt_bound(item.get("max_profit")), className="table-cell"),
                    html.Div(fmt_bound(item.get("max_loss")), className="table-cell"),
                    html.Div(f"{float(item.get('score') or 0.0):.2f}", className="table-cell"),
                ],
            )
        )
    return html.Div(rows, className="mini-table suite-results-table")


def build_strategy_suite_flow_summary(flow_df):
    if flow_df is None or flow_df.empty:
        return html.Div("Live options flow will appear here when recent Deribit trades are available.", className="suite-empty")
    total_premium = float(pd.to_numeric(flow_df["premium_usd"], errors="coerce").fillna(0.0).sum())
    total_contracts = float(pd.to_numeric(flow_df["contracts"], errors="coerce").fillna(0.0).sum())
    buy_count = int((flow_df["direction"].astype(str).str.lower() == "buy").sum())
    sell_count = int((flow_df["direction"].astype(str).str.lower() == "sell").sum())
    top_row = flow_df.iloc[0]
    cards = [
        make_suite_card("Recent Prints", f"{len(flow_df):,}"),
        make_suite_card("Premium", fmt_money(total_premium, 0)),
        make_suite_card("Contracts", f"{total_contracts:,.0f}"),
        make_suite_card("Aggressor Mix", f"{buy_count} / {sell_count}", "buys / sells"),
        make_suite_card("Latest Trade", str(top_row.get("instrument_name") or "n/a"), fmt_money(top_row.get("premium_usd"), 0)),
    ]
    return html.Div(cards, className="suite-card-grid")


def build_strategy_suite_flow_figure(flow_df):
    if flow_df is None or flow_df.empty:
        return build_empty_figure("No recent BTC / ETH option trades matched the flow filters.")

    work = flow_df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work["premium_usd"] = pd.to_numeric(work["premium_usd"], errors="coerce").fillna(0.0)
    work["strike"] = pd.to_numeric(work["strike"], errors="coerce")

    p = bokeh_figure(
        height=320, sizing_mode="stretch_width",
        x_axis_type="datetime",
        x_axis_label="Trade Time (UTC)", y_axis_label="Strike",
        tools="pan,wheel_zoom,box_zoom,reset,crosshair,hover",
        active_scroll="wheel_zoom",
        title="Recent Option Prints",
    )
    p.title.text_color = "#eaecf0"
    p.title.text_font = _BK_FONT
    p.title.text_font_size = "13px"

    for direction, color in (("buy", "#4ec88c"), ("sell", "#ff7f8d")):
        subset = work[work["direction"].astype(str).str.lower() == direction].copy()
        if subset.empty:
            continue
        sizes = subset["premium_usd"].clip(lower=1.0).pow(0.35).clip(lower=10.0, upper=32.0)
        src = ColumnDataSource(dict(
            x=subset["timestamp"].tolist(),
            y=subset["strike"].tolist(),
            size=sizes.tolist(),
            instrument=subset["instrument_name"].tolist(),
            premium=subset["premium_usd"].tolist(),
        ))
        p.scatter("x", "y", source=src, size="size", color=color, alpha=0.82,
                   line_color="#04090f", line_alpha=0.92, line_width=1,
                   legend_label=direction.title())

    # Configure hover with correct datetime formatter
    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Instrument", "@instrument"),
            ("Time",       "@x{%Y-%m-%d %H:%M UTC}"),
            ("Strike",     "@y{0,0}"),
            ("Premium",    "@premium{$0,0}"),
        ]
        hover.formatters = {"@x": "datetime"}
        hover.mode = "mouse"

    p.xaxis.formatter = DatetimeTickFormatter(
        hours="%H:%M", days="%m-%d", months="%b %Y"
    )
    p.yaxis.formatter = NumeralTickFormatter(format="0,0")
    p.legend.orientation = "horizontal"
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    _bk_theme(p)
    return _bk_html(p)


def build_strategy_suite_flow_table(flow_df):
    if flow_df is None or flow_df.empty:
        return html.Div("No flow rows matched the current filters.", className="suite-empty")
    rows = [
        html.Div(
            className="table-row table-header",
            children=[
                html.Div("Time", className="table-cell"),
                html.Div("Instrument", className="table-cell"),
                html.Div("Side", className="table-cell"),
                html.Div("Type", className="table-cell"),
                html.Div("Contracts", className="table-cell"),
                html.Div("Premium", className="table-cell"),
                html.Div("IV", className="table-cell"),
            ],
        )
    ]
    for _, row in flow_df.head(12).iterrows():
        ts = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        time_label = ts.strftime("%m-%d %H:%M") if pd.notna(ts) else "n/a"
        rows.append(
            html.Div(
                className="table-row",
                children=[
                    html.Div(time_label, className="table-cell"),
                    html.Div(str(row.get("instrument_name") or "n/a"), className="table-cell"),
                    html.Div(str(row.get("direction") or "").title(), className="table-cell"),
                    html.Div(str(row.get("type") or "").upper(), className="table-cell"),
                    html.Div(f"{float(row.get('contracts') or 0.0):,.0f}", className="table-cell"),
                    html.Div(fmt_money(row.get("premium_usd"), 0), className="table-cell"),
                    html.Div(fmt_pct(row.get("iv")), className="table-cell"),
                ],
            )
        )
    return html.Div(rows, className="mini-table suite-flow-table")


def build_strategy_suite_panel():
    return html.Div(
        id="section-strategy",
        className="panel strategy-suite-panel-shell",
        children=[
            html.Div(
                className="panel-head",
                children=[
                    html.Div(
                        className="panel-head-copy",
                        children=[
                            html.H3("Crypto Strategy Suite", className="panel-title"),
                            html.Span("OptionStrat-style builder, optimizer, and live BTC / ETH flow", className="panel-subtitle"),
                        ],
                    ),
                    html.Div(
                        className="strategy-suite-meta",
                        children=[
                            html.Button("Refresh Chain", id="strategy-suite-refresh-chain-btn", className="action-button secondary"),
                            html.Div(id="strategy-suite-chain-note", className="strategy-suite-note"),
                            html.Button("\u25bc", className="panel-collapse-btn"),
                        ],
                    ),
                ],
            ),
            dcc.Tabs(
                parent_className="suite-tabs-wrap",
                className="suite-tabs",
                children=[
                    dcc.Tab(
                        label="Builder",
                        className="suite-tab",
                        selected_className="suite-tab suite-tab-selected",
                        children=[
                            html.Div(
                                className="strategy-suite-body",
                                children=[
                                    html.Div(
                                        className="strategy-suite-controls",
                                        children=[
                                            html.Div(
                                                [
                                                    html.Div("Template", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-builder-template",
                                                        options=[{"label": label, "value": value} for value, label in DEFAULT_STRATEGY_TEMPLATES],
                                                        value="long_call",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Saved", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-builder-saved",
                                                        options=[],
                                                        placeholder="Saved strategies",
                                                        clearable=True,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Save Name", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-builder-save-name",
                                                        type="text",
                                                        placeholder="e.g. BTC swing call",
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Commission", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-builder-commission",
                                                        type="number",
                                                        min=0,
                                                        step=0.5,
                                                        value=DEFAULT_COMMISSION_PER_CONTRACT,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Eval Days", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-builder-eval-days",
                                                        type="number",
                                                        min=0,
                                                        step=1,
                                                        value=7,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Actions", className="control-label"),
                                                    html.Div(
                                                        className="suite-actions",
                                                        children=[
                                                            html.Button("Load", id="strategy-suite-builder-load-btn", className="action-button secondary"),
                                                            html.Button("Save", id="strategy-suite-builder-save-btn", className="action-button"),
                                                            html.Button("Delete", id="strategy-suite-builder-delete-btn", className="action-button secondary"),
                                                            html.Button("Refresh", id="strategy-suite-builder-refresh-btn", className="action-button secondary", title="Reseed legs from current chain"),
                                                        ],
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                        ],
                                    ),
                                    html.Div(id="strategy-suite-builder-status", className="suite-status"),
                                    html.Div(
                                        className="suite-subgrid",
                                        children=[
                                            html.Div(
                                                className="suite-subpanel",
                                                children=[
                                                    html.Div("Leg Builder", className="suite-subpanel-title"),
                                                    html.Div(id="strategy-suite-builder-legs", className="suite-legs"),
                                                ],
                                            ),
                                            html.Div(
                                                className="suite-subpanel",
                                                children=[
                                                    html.Div("Snapshot", className="suite-subpanel-title"),
                                                    html.Div(id="strategy-suite-builder-summary"),
                                                    html.Div(id="strategy-suite-builder-metrics"),
                                                ],
                                            ),
                                        ],
                                    ),
                                    html.Div(
                                        className="suite-subgrid",
                                        children=[
                                            html.Div(
                                                className="suite-subpanel",
                                                children=[
                                                    html.Div("Payoff", className="suite-subpanel-title"),
                                                    html.Iframe(id="strategy-suite-builder-chart", className="suite-figure", style=_IFRAME_STYLE),
                                                ],
                                            ),
                                            html.Div(
                                                className="suite-subpanel",
                                                children=[
                                                    html.Div("Scenarios", className="suite-subpanel-title"),
                                                    html.Div(id="strategy-suite-builder-scenarios"),
                                                ],
                                            ),
                                        ],
                                    ),
                                ],
                            )
                        ],
                    ),
                    dcc.Tab(
                        label="Optimizer",
                        className="suite-tab",
                        selected_className="suite-tab suite-tab-selected",
                        children=[
                            html.Div(
                                className="strategy-suite-body",
                                children=[
                                    html.Div(
                                        className="strategy-suite-controls",
                                        children=[
                                            html.Div(
                                                [
                                                    html.Div("Bias", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-optimizer-bias",
                                                        options=[
                                                            {"label": "Bullish", "value": "bullish"},
                                                            {"label": "Bearish", "value": "bearish"},
                                                            {"label": "Neutral", "value": "neutral"},
                                                            {"label": "Volatility", "value": "volatility"},
                                                        ],
                                                        value="bullish",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Objective", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-optimizer-objective",
                                                        options=[
                                                            {"label": "Balanced", "value": "balanced"},
                                                            {"label": "Chance Of Profit", "value": "chance"},
                                                            {"label": "Max Return", "value": "max_return"},
                                                        ],
                                                        value="balanced",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Max Cost (% Spot)", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-optimizer-max-cost",
                                                        type="number",
                                                        min=0,
                                                        step=0.01,
                                                        value=0.2,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Min POP", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-optimizer-min-pop",
                                                        type="number",
                                                        min=0,
                                                        max=1,
                                                        step=0.05,
                                                        value=0.35,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Eval Days", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-optimizer-eval-days",
                                                        type="number",
                                                        min=0,
                                                        step=1,
                                                        value=7,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Run", className="control-label"),
                                                    html.Div(className="suite-actions", children=[html.Button("Rescore", id="strategy-suite-optimizer-run-btn", className="action-button")]),
                                                ],
                                                className="control-stack",
                                            ),
                                        ],
                                    ),
                                    html.Div(id="strategy-suite-optimizer-status", className="suite-status"),
                                    html.Div(className="suite-subpanel", children=[html.Div("Best Match", className="suite-subpanel-title"), html.Div(id="strategy-suite-optimizer-summary")]),
                                    html.Div(
                                        className="suite-subgrid",
                                        children=[
                                            html.Div(className="suite-subpanel", children=[html.Div("Optimizer Map", className="suite-subpanel-title"), html.Iframe(id="strategy-suite-optimizer-chart", className="suite-figure", style={"width":"100%","height":"360px","border":"none","display":"block"})]),
                                            html.Div(className="suite-subpanel", children=[html.Div("Top Candidates", className="suite-subpanel-title"), html.Div(id="strategy-suite-optimizer-table")]),
                                        ],
                                    ),
                                ],
                            )
                        ],
                    ),
                    dcc.Tab(
                        label="Flow",
                        className="suite-tab",
                        selected_className="suite-tab suite-tab-selected",
                        children=[
                            html.Div(
                                className="strategy-suite-body",
                                children=[
                                    html.Div(
                                        className="strategy-suite-controls",
                                        children=[
                                            html.Div(
                                                [
                                                    html.Div("Trades", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-flow-count",
                                                        type="number",
                                                        min=10,
                                                        max=150,
                                                        step=10,
                                                        value=60,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Min Premium", className="control-label"),
                                                    dcc.Input(
                                                        id="strategy-suite-flow-min-premium",
                                                        type="number",
                                                        min=0,
                                                        step=5000,
                                                        value=25000,
                                                        className="text-input",
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Option Type", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-flow-type",
                                                        options=[
                                                            {"label": "All", "value": "all"},
                                                            {"label": "Calls", "value": "call"},
                                                            {"label": "Puts", "value": "put"},
                                                        ],
                                                        value="all",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Direction", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="strategy-suite-flow-direction",
                                                        options=[
                                                            {"label": "All", "value": "all"},
                                                            {"label": "Aggressor Buy", "value": "buy"},
                                                            {"label": "Aggressor Sell", "value": "sell"},
                                                        ],
                                                        value="all",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="control-stack",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div("Refresh", className="control-label"),
                                                    html.Div(className="suite-actions", children=[html.Button("Reload Flow", id="strategy-suite-flow-refresh-btn", className="action-button secondary")]),
                                                ],
                                                className="control-stack",
                                            ),
                                        ],
                                    ),
                                    html.Div(id="strategy-suite-flow-status", className="suite-status"),
                                    html.Div(className="suite-subpanel", children=[html.Div("Flow Summary", className="suite-subpanel-title"), html.Div(id="strategy-suite-flow-summary")]),
                                    html.Div(
                                        className="suite-subgrid",
                                        children=[
                                            html.Div(className="suite-subpanel", children=[html.Div("Flow Map", className="suite-subpanel-title"), html.Iframe(id="strategy-suite-flow-chart", className="suite-figure", style=_IFRAME_STYLE)]),
                                            html.Div(className="suite-subpanel", children=[html.Div("Recent Prints", className="suite-subpanel-title"), html.Div(id="strategy-suite-flow-table")]),
                                        ],
                                    ),
                                ],
                            )
                        ],
                    ),
                ],
            ),
        ],
    )


def build_option_heatmap_tool(selected_df):
    if selected_df is None or selected_df.empty:
        return build_empty_figure("No option heatmap available"), "Heatmap unavailable: no selected rows", "n/a"

    work = selected_df.copy()
    work["expiry"] = pd.to_datetime(work["expiry"]).dt.normalize()
    for col in ("strike", "call_oi", "put_oi", "call_gex", "put_gex"):
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0.0)
    work["total_oi"] = work["call_oi"] + work["put_oi"]
    work["total_gex"] = work["call_gex"] + work["put_gex"]

    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    work["dte"] = (work["expiry"] - today).dt.days.astype(int)
    forward = work[work["dte"] >= 0].copy()
    if forward.empty:
        return build_empty_figure("No forward expiries for heatmap"), "Heatmap unavailable: no forward expiries", "n/a"

    grouped = (
        forward.groupby(["expiry", "dte", "strike"], as_index=False)
        .agg(
            total_oi=("total_oi", "sum"),
            net_gex=("total_gex", "sum"),
        )
        .sort_values(["expiry", "strike"])
    )
    grouped = grouped[grouped["total_oi"] > 0]
    if grouped.empty:
        return build_empty_figure("No open interest values available"), "Heatmap unavailable: no positive OI", "n/a"

    grouped["expiry_label"] = grouped["expiry"].dt.strftime("%Y-%m-%d") + " (" + grouped["dte"].astype(str) + "d)"
    expiry_order = (
        grouped[["expiry", "expiry_label"]]
        .drop_duplicates()
        .sort_values("expiry")
        .head(14)["expiry_label"]
        .tolist()
    )
    grouped = grouped[grouped["expiry_label"].isin(expiry_order)].copy()

    strike_totals = grouped.groupby("strike", as_index=False)["total_oi"].sum().sort_values("total_oi", ascending=False)
    if len(strike_totals) > 100:
        keep_strikes = set(strike_totals.head(100)["strike"].tolist())
        grouped = grouped[grouped["strike"].isin(keep_strikes)].copy()

    strike_order = sorted(grouped["strike"].unique().tolist())
    if not strike_order or not expiry_order:
        return build_empty_figure("No valid strike/expiry matrix for heatmap"), "Heatmap unavailable: invalid matrix", "n/a"

    pivot = (
        grouped.pivot_table(
            index="strike",
            columns="expiry_label",
            values="total_oi",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reindex(index=strike_order, columns=expiry_order, fill_value=0.0)
    )
    if pivot.empty:
        return build_empty_figure("No heatmap pivot available"), "Heatmap unavailable: empty pivot", "n/a"

    z_cap = float(grouped["total_oi"].quantile(0.97))
    if not pd.notna(z_cap) or z_cap <= 0:
        z_cap = float(grouped["total_oi"].max())
    z_cap = max(z_cap, 1.0)

    expiry_labels = pivot.columns.tolist()
    # Strike labels sorted high→low so highest strike is at top of heatmap
    strike_vals_sorted = list(reversed(sorted(pivot.index.tolist())))
    strike_labels = [fmt_price(val) for val in strike_vals_sorted]

    # Build flat arrays: one row per (expiry, strike) cell
    strike_label_map = {v: fmt_price(v) for v in pivot.index.tolist()}
    xs, ys, zs, raw_strikes = [], [], [], []
    for ei, exp in enumerate(expiry_labels):
        for sv in strike_vals_sorted:
            xs.append(exp)
            ys.append(strike_label_map[sv])
            zs.append(float(pivot.loc[sv, exp]) if exp in pivot.columns else 0.0)
            raw_strikes.append(sv)

    hm_palette = ["#0f1924", "#1a2b3c", "#26557d", "#4e8fbc", "#f0aa4d", "#ffe0b7"]
    mapper = LinearColorMapper(palette=hm_palette, low=0, high=z_cap)
    src = ColumnDataSource(dict(x=xs, y=ys, z=zs, strike=raw_strikes))

    # Adapt font size to number of strikes so labels don't overlap
    n_strikes = len(strike_labels)
    label_fs = "8px" if n_strikes > 60 else ("9px" if n_strikes > 35 else "10px")

    p = bokeh_figure(
        height=440, sizing_mode="stretch_width",
        x_range=expiry_labels,
        y_range=strike_labels,          # already ordered high→low
        x_axis_label="Expiry (DTE)", y_axis_label="Strike",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
    )
    p.rect(x="x", y="y", width=0.96, height=0.96, source=src,
           fill_color={"field": "z", "transform": mapper},
           line_color="#071019", line_width=0.5, line_alpha=0.4)

    color_bar = ColorBar(
        color_mapper=mapper, width=10, label_standoff=5,
        border_line_color="#92aac6", border_line_alpha=0.18,
        background_fill_color="#0b1620",
        major_label_text_color="#a8b7c7",
        major_label_text_font=_BK_MONO,
        major_label_text_font_size="9px",
        title="OI", title_text_color="#a8b7c7",
        title_text_font=_BK_FONT,
        title_text_font_size="10px",
        formatter=NumeralTickFormatter(format="0,0"),
    )
    p.add_layout(color_bar, "right")

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Expiry",    "@x"),
            ("Strike",    "@strike{0,0}"),
            ("Total OI",  "@z{0,0}"),
        ]

    p.xaxis.major_label_orientation = -0.52
    p.xaxis.major_label_text_font = _BK_MONO
    p.xaxis.major_label_text_font_size = "9px"
    p.yaxis.major_label_text_font = _BK_MONO
    p.yaxis.major_label_text_font_size = label_fs
    _bk_theme(p)
    fig = _bk_html(p)

    hottest = grouped.loc[grouped["total_oi"].idxmax()]
    hot_expiry = pd.Timestamp(hottest["expiry"]).strftime("%Y-%m-%d")
    hot_strike = float(hottest["strike"])
    hot_oi = float(hottest["total_oi"])
    hot_gex = float(hottest["net_gex"])
    summary = (
        f"Heat wall: strike {fmt_price(hot_strike)} @ {hot_expiry} | "
        f"OI {fmt_metric(hot_oi)} | Net GEX {fmt_metric(hot_gex)} | "
        f"{len(expiry_order)} expiries shown"
    )
    return fig, summary, fmt_price(hot_strike)


def build_vanna_charm_chart(chain_df, spot, top_n=20):
    """Stacked bar chart of dealer-side vanna and charm exposure by strike."""
    from bokeh.models import LinearAxis, Range1d, NumeralTickFormatter, HoverTool, Span, Label

    if chain_df is None or chain_df.empty:
        return build_empty_figure("Vanna / charm exposure unavailable: no chain data"), "n/a"

    exposures = compute_chain_exposures(chain_df, spot_override=spot)
    if exposures.empty:
        return build_empty_figure("Vanna / charm exposure unavailable: no priceable strikes"), "n/a"

    grouped = aggregate_by_strike(exposures, top_n=top_n)
    if grouped.empty:
        return build_empty_figure("Vanna / charm exposure unavailable: no strikes to plot"), "n/a"

    # Dealer view = invert customer-side exposure
    grouped["vanna_dealer"] = -grouped["vanna_exposure"]
    grouped["charm_dealer"] = -grouped["charm_exposure"]
    grouped["strike_label"] = grouped["strike"].apply(fmt_price)
    strike_labels = grouped["strike_label"].tolist()

    src = ColumnDataSource(dict(
        strike=strike_labels,
        strike_raw=grouped["strike"].tolist(),
        vanna=grouped["vanna_dealer"].tolist(),
        charm=grouped["charm_dealer"].tolist(),
        vanna_color=["#13b955" if v >= 0 else "#ea3943" for v in grouped["vanna_dealer"]],
    ))

    vanna_max = max(abs(grouped["vanna_dealer"].min()), abs(grouped["vanna_dealer"].max()), 1.0)
    charm_max = max(abs(grouped["charm_dealer"].min()), abs(grouped["charm_dealer"].max()), 1.0)
    vanna_pad = vanna_max * 0.18
    charm_pad = charm_max * 0.18

    p = bokeh_figure(
        height=320,
        sizing_mode="stretch_width",
        x_range=strike_labels,
        y_range=Range1d(start=-vanna_max - vanna_pad, end=vanna_max + vanna_pad),
        x_axis_label="Strike",
        y_axis_label="Vanna Exposure ($/vol·pt/1%)",
        tools="pan,wheel_zoom,box_zoom,reset,hover",
        active_scroll="wheel_zoom",
        toolbar_location="right",
    )
    p.extra_y_ranges = {"charm": Range1d(start=-charm_max - charm_pad, end=charm_max + charm_pad)}
    p.add_layout(LinearAxis(y_range_name="charm", axis_label="Charm ($ delta·decay/day)"), "right")

    p.vbar(
        x="strike", top="vanna", source=src,
        width=0.62, color="vanna_color", line_color=None,
        legend_label="Vanna (dealer)", fill_alpha=0.85,
    )
    p.scatter(
        x="strike", y="charm", source=src, y_range_name="charm",
        size=9, marker="diamond",
        fill_color="#5b8dea", line_color="#0b0e14", line_width=1,
        legend_label="Charm (dealer)",
    )
    p.line(
        x="strike", y="charm", source=src, y_range_name="charm",
        line_color="#5b8dea", line_alpha=0.45, line_width=1.5,
    )

    p.add_layout(Span(location=0, dimension="width", line_color="#8a92a6", line_alpha=0.4, line_width=1, line_dash="dashed"))

    if spot and spot > 0:
        try:
            spot_label = fmt_price(spot)
            if spot_label in strike_labels:
                p.add_layout(Span(location=spot_label, dimension="height",
                                  line_color="#75c4ff", line_alpha=0.5, line_width=1, line_dash="dotted"))
        except Exception:
            pass

    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Strike", "@strike_raw{0,0}"),
            ("Vanna ($)", "@vanna{$0,0}"),
            ("Charm ($/d)", "@charm{$0,0}"),
        ]

    p.yaxis[0].formatter = NumeralTickFormatter(format="$0,0.[0]a")
    for ax in p.yaxis:
        ax.formatter = NumeralTickFormatter(format="$0,0.[0]a")
    p.xaxis.major_label_orientation = 0.9
    p.xaxis.major_label_text_font = _BK_MONO
    p.xaxis.major_label_text_font_size = "8px"
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.legend.click_policy = "hide"
    _bk_theme(p)

    total_vanna = float(grouped["vanna_dealer"].sum())
    total_charm = float(grouped["charm_dealer"].sum())
    summary = (
        f"Σ Vanna {fmt_metric(total_vanna)}$/vol·1% · "
        f"Σ Charm {fmt_metric(total_charm)}$/day · "
        f"{len(grouped)} strikes (top by |exposure|)"
    )
    return _bk_html(p), summary


def build_dealer_flow_panel_children(filtered_df, spot):
    """Render the Dealer Hedge Flow panel as a small grid of shock cards."""
    flow = compute_dealer_hedge_flow(filtered_df, spot)
    shocks = flow.get("shocks") or []
    net_gex = flow.get("net_gex") or 0.0
    regime = flow.get("regime") or "Neutral"

    regime_class = "pill-pos" if regime == "Mean-Reverting" else (
        "pill-neg" if regime == "Trend-Following" else "pill-neutral"
    )

    header = html.Div(
        className="dealer-flow-summary",
        children=[
            html.Div(
                className="dealer-flow-stat",
                children=[
                    html.Div("Regime", className="k"),
                    html.Div(regime, className=f"v dealer-flow-regime {regime_class}"),
                ],
            ),
            html.Div(
                className="dealer-flow-stat",
                children=[
                    html.Div("Net GEX", className="k"),
                    html.Div(
                        f"{'+' if net_gex >= 0 else ''}{fmt_metric(net_gex)}$",
                        className=f"v {'up' if net_gex >= 0 else 'down'}",
                    ),
                ],
            ),
            html.Div(
                className="dealer-flow-stat",
                children=[
                    html.Div("Spot", className="k"),
                    html.Div(fmt_price(spot or 0), className="v"),
                ],
            ),
        ],
    )

    cards = []
    for s in shocks:
        pct = float(s.get("pct") or 0)
        hedge_usd = float(s.get("hedge_usd") or 0)
        hedge_units = float(s.get("hedge_units") or 0)
        direction = s.get("direction") or "neutral"
        pct_label = f"{'+' if pct > 0 else ''}{pct * 100:.0f}%"
        if direction == "buy":
            tone, arrow = "up", "↑"
        elif direction == "sell":
            tone, arrow = "down", "↓"
        else:
            tone, arrow = "neutral", "·"
        cards.append(
            html.Div(
                className=f"dealer-flow-card tone-{tone}",
                children=[
                    html.Div(f"Spot {pct_label}", className="k"),
                    html.Div(
                        [
                            html.Span(arrow, className="dealer-flow-arrow"),
                            html.Span(direction.upper(), className="dealer-flow-direction"),
                        ],
                        className="dealer-flow-action",
                    ),
                    html.Div(f"{fmt_metric(abs(hedge_usd))}$", className="v"),
                    html.Div(
                        f"{abs(hedge_units):,.2f} units",
                        className="dealer-flow-units",
                    ),
                ],
            )
        )

    return [header, html.Div(className="dealer-flow-grid", children=cards)]


def timeframe_label(value):
    for option in DERIBIT_TIMEFRAME_OPTIONS:
        if option["value"] == value:
            return option["label"]
    return str(value)


def resolution_to_minutes(value):
    return DERIBIT_RESOLUTION_TO_MINUTES.get(str(value), 1)


_CANDLE_CACHE_TTL = 25  # seconds
_CANDLE_CACHE = TTLCache(ttl_seconds=_CANDLE_CACHE_TTL, max_entries=64)


def fetch_deribit_candles(
    instrument_name="BTC-PERPETUAL",
    resolution=DERIBIT_RESOLUTION,
    lookback_minutes=DERIBIT_LOOKBACK_MINUTES,
    start_ts=None,
    end_ts=None,
):
    # Return cached candles if fresh enough
    cache_key = (instrument_name, str(resolution), int(lookback_minutes))
    if start_ts is None and end_ts is None:
        cached = _CANDLE_CACHE.get(cache_key)
        if cached is not None:
            return cached
    def to_millis(ts_val):
        if ts_val is None:
            return None
        if isinstance(ts_val, (int, float)):
            return int(ts_val)
        ts = pd.Timestamp(ts_val)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.timestamp() * 1000)

    end_ts_ms = to_millis(end_ts) or int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
    start_ts_ms = to_millis(start_ts)
    if start_ts_ms is None:
        start_ts_ms = end_ts_ms - (int(lookback_minutes) * 60 * 1000)
    if start_ts_ms > end_ts_ms:
        start_ts_ms, end_ts_ms = end_ts_ms, start_ts_ms
    query = urllib.parse.urlencode(
        {
            "instrument_name": instrument_name,
            "start_timestamp": start_ts_ms,
            "end_timestamp": end_ts_ms,
            "resolution": str(resolution),
        }
    )
    url = f"{DERIBIT_API_URL}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": "gex-dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=3) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    result = payload.get("result", {})
    ticks = result.get("ticks", [])
    opens = result.get("open", [])
    highs = result.get("high", [])
    lows = result.get("low", [])
    closes = result.get("close", [])
    status = result.get("status", "ok")

    if status != "ok":
        return []

    count = min(len(ticks), len(opens), len(highs), len(lows), len(closes))
    if count == 0:
        return []

    candles = []
    for i in range(count):
        candles.append(
            {
                "t": pd.to_datetime(ticks[i], unit="ms", utc=True),
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
            }
        )
    if start_ts is None and end_ts is None:
        _CANDLE_CACHE.set(cache_key, candles)
    return candles


def _draw_candlesticks(p, candles_df, asset_symbol, bar_width_ms):
    """Add candlestick glyphs (segment wicks + vbar bodies) to a Bokeh figure."""
    inc = candles_df[candles_df["close"] >= candles_df["open"]]
    dec = candles_df[candles_df["close"] < candles_df["open"]]
    # Wicks
    p.segment(candles_df["t"], candles_df["high"], candles_df["t"], candles_df["low"],
              color="#a8b7c7", line_width=1, line_alpha=0.7)
    # Bodies
    p.vbar(inc["t"], width=bar_width_ms * 0.8, top=inc["close"], bottom=inc["open"],
           fill_color="#4ec88c", line_color="#4ec88c")
    p.vbar(dec["t"], width=bar_width_ms * 0.8, top=dec["open"], bottom=dec["close"],
           fill_color="#ff7f8d", line_color="#ff7f8d")


def build_spot_figure(
    history_points,
    levels,
    deribit_candles=None,
    max_bars=DEFAULT_SESSION_BARS,
    asset_symbol="BTC",
    fill_gaps=False,
    smooth_window=0,
    fixed_range=None,
):
    p = bokeh_figure(
        sizing_mode="stretch_both",
        x_axis_type="datetime",
        x_axis_label="Time (session)", y_axis_label="Price",
        tools="pan,wheel_zoom,box_zoom,reset,crosshair,hover",
        active_scroll="wheel_zoom",
        y_axis_location="right",
    )
    latest_spot = levels.get("spot")
    price_lows = []
    price_highs = []
    right_labels = []

    if deribit_candles:
        candles_df = pd.DataFrame(deribit_candles).dropna()
        candles_df = candles_df.sort_values("t").drop_duplicates(subset="t")
        if max_bars and len(candles_df) > int(max_bars):
            candles_df = candles_df.tail(int(max_bars))
        if len(candles_df) >= 2:
            price_lows.extend(candles_df["low"].astype(float).tolist())
            price_highs.extend(candles_df["high"].astype(float).tolist())
            # Estimate bar width from median time delta (in ms for Bokeh datetime axis)
            deltas = candles_df["t"].diff().dt.total_seconds().dropna()
            med_secs = float(deltas.median()) if not deltas.empty else 60.0
            bar_ms = med_secs * 1000.0
            _draw_candlesticks(p, candles_df, asset_symbol, bar_ms)
            latest_spot = float(candles_df["close"].iloc[-1])
    elif history_points:
        hist_df = pd.DataFrame(history_points)
        hist_df["t"] = pd.to_datetime(hist_df["t"], utc=True)
        hist_df = hist_df.sort_values("t").drop_duplicates(subset="t")
        if max_bars and len(hist_df) > int(max_bars):
            hist_df = hist_df.tail(int(max_bars))
        price_series = hist_df.set_index("t")["p"].astype(float)
        if smooth_window and smooth_window > 1:
            price_series = price_series.rolling(window=int(smooth_window), min_periods=1).median()
        if fill_gaps and not price_series.empty:
            full_index = pd.date_range(
                start=price_series.index.min(),
                end=price_series.index.max(),
                freq="1min",
                tz="UTC",
            )
            price_series = price_series.reindex(full_index)
            price_series = price_series.interpolate(method="time").ffill().bfill()
        price_series.index.name = "t"
        candles = price_series.resample("1min").ohlc().dropna().reset_index()
        if max_bars and len(candles) > int(max_bars):
            candles = candles.tail(int(max_bars))

        if len(candles) >= 2:
            price_lows.extend(candles["low"].astype(float).tolist())
            price_highs.extend(candles["high"].astype(float).tolist())
            bar_ms = 60.0 * 1000.0  # 1-min candles
            _draw_candlesticks(p, candles, asset_symbol, bar_ms)
        else:
            price_lows.extend(hist_df["p"].astype(float).tolist())
            price_highs.extend(hist_df["p"].astype(float).tolist())
            p.line(hist_df["t"], hist_df["p"], color="#eaecf0", line_width=1.8)
        if not hist_df.empty:
            latest_spot = float(hist_df["p"].iloc[-1])

    _DASH_MAP = {"solid": "solid", "dash": "dashed", "dot": "dotted"}
    level_lines = [
        ("P1", levels.get("p1"), "#13b955", "solid",  1.4),
        ("P2", levels.get("p2"), "#13b955", "dashed", 1.2),
        ("AG 1", levels.get("a1"), "#5b8dea", [8, 4, 2, 4], 1.3),
        ("AG 2", levels.get("a2"), "#5b8dea", [8, 4, 2, 4], 1.3),
        ("MP", levels.get("mp"), "#5b8dea", "dotted", 1.5),
        ("GF", levels.get("flip"), "#ffffff", [6, 3],  1.5),
        ("N1", levels.get("n1"), "#ea3943", "solid",  1.4),
        ("N2", levels.get("n2"), "#ea3943", "dashed", 1.2),
    ]
    level_values = [float(v) for _, v, *_ in level_lines if v is not None and pd.notna(v)]

    for lbl, value, color, dash, width in level_lines:
        if value is None or not pd.notna(value):
            continue
        y = float(value)
        line_dash = dash if isinstance(dash, list) else _DASH_MAP.get(dash, "solid")
        p.add_layout(Span(location=y, dimension="width", line_color=color, line_width=width,
                          line_dash=line_dash, line_alpha=0.9))
        right_labels.append({"text": f"{lbl}  {fmt_price(y)}", "y": y, "color": color})

    if latest_spot is not None and pd.notna(latest_spot):
        spot_y = float(latest_spot)
        p.add_layout(Span(location=spot_y, dimension="width", line_color="#ffffff",
                          line_width=1.8, line_dash="dashed", line_alpha=0.9))
        right_labels.append({"text": f"SP  {fmt_price(spot_y)}", "y": spot_y, "color": "#ffffff"})

    # Compute y-axis range
    envelope_values = list(price_lows) + list(price_highs) + level_values
    if latest_spot is not None and pd.notna(latest_spot):
        envelope_values.append(float(latest_spot))
    if envelope_values:
        env_min = min(envelope_values)
        env_max = max(envelope_values)
        span_v = max(env_max - env_min, max(abs(env_max), 1.0) * 0.02)
        pad = span_v * 0.08
        y_min = max(0.0, env_min - pad)
        y_max = env_max + pad
        if y_max <= y_min:
            y_max = y_min + max(abs(y_min) * 0.01, 1.0)
        if fixed_range and len(fixed_range) == 2:
            fr_min, fr_max = fixed_range
            if fr_min is not None and fr_max is not None and fr_max > fr_min:
                y_min, y_max = float(fr_min), float(fr_max)
        p.y_range = Range1d(y_min, y_max)

    # ---- Stagger right-side price labels to avoid overlap ----
    # Labels are drawn inside the chart at the right x edge using text glyphs.
    if right_labels and envelope_values:
        y_range_span = max(y_max - y_min, 1.0)
        # Minimum vertical gap: roughly the height of one label (~0.8% of price range per px)
        min_gap = y_range_span * 0.028
        sorted_lbls = sorted(right_labels, key=lambda item: item["y"])
        adj_y = [item["y"] for item in sorted_lbls]

        # Push upward pass: each label must be at least min_gap above the previous
        cursor = y_min + y_range_span * 0.01
        for i in range(len(adj_y)):
            adj_y[i] = max(sorted_lbls[i]["y"], cursor)
            cursor = adj_y[i] + min_gap

        # Push downward pass: clamp top labels back within range
        cursor = y_max - y_range_span * 0.01
        for i in range(len(adj_y) - 1, -1, -1):
            adj_y[i] = min(adj_y[i], cursor)
            cursor = adj_y[i] - min_gap

        # Draw each label as a Bokeh Label anchored to the right of the data range
        for i, item in enumerate(sorted_lbls):
            p.add_layout(Label(
                x=1.0, y=adj_y[i],
                x_units="screen", y_units="data",
                x_offset=-4,
                text=item["text"],
                text_color=item["color"],
                text_font=_BK_MONO,
                text_font_size="10px",
                text_align="right",
                text_baseline="middle",
                background_fill_color="#141720",
                background_fill_alpha=0.92,
                border_line_color=item["color"],
                border_line_alpha=0.7,
                border_line_width=1,
            ))

    p.yaxis.formatter = NumeralTickFormatter(format="0,0")
    # crosshair is already in the tools string — don't add a second one
    hover = p.select_one(HoverTool)
    if hover:
        hover.tooltips = [
            ("Time",  "$x{%Y-%m-%d %H:%M UTC}"),
            ("Price", "$y{0,0}"),
        ]
        hover.formatters = {"$x": "datetime"}
        hover.mode = "vline"
    p.toolbar.logo = None
    _bk_theme(p)
    return _bk_html(p)


def serialize_positions(df: pd.DataFrame) -> list:
    if df is None or df.empty:
        return []
    out = df.copy()
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.where(pd.notna(out), None)
    return out.to_dict("records")


POSITIONS_SEED = serialize_positions(load_positions(POSITIONS_FILE))
df_all = load_data()
app = dash.Dash(__name__)
app.title = "GEX Analysis Dashboard"
app.index_string = f"""<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{THEME_CSS}</style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
        <script>
        document.addEventListener('click', function(e) {{
            var btn = e.target.closest('.panel-collapse-btn');
            if (!btn) return;
            var panel = btn.closest('.panel');
            if (!panel) return;
            panel.classList.toggle('panel-collapsed');
            btn.textContent = panel.classList.contains('panel-collapsed') ? '\u25b6' : '\u25bc';
        }});
        // Status bar live telemetry — mirrors values from existing cards / dropdowns
        (function() {{
            function pad(n) {{ return n < 10 ? '0' + n : '' + n; }}
            function fmtTime(d) {{
                return pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()) + 'Z';
            }}
            var lastSpot = null;
            var lastSpotChangeAt = Date.now();
            function tick() {{
                try {{
                    var spotEl = document.getElementById('spot-card');
                    var symEl = document.querySelector('#symbol .Select-value-label');
                    var modeRoot = document.getElementById('data-mode');
                    var sbSpot = document.getElementById('status-bar-spot');
                    var sbSym = document.getElementById('status-bar-symbol');
                    var sbMode = document.getElementById('status-bar-mode');
                    var sbClock = document.getElementById('status-bar-clock');
                    var sbDot = document.getElementById('status-bar-conn-dot');
                    var sbLat = document.getElementById('status-bar-latency');
                    if (spotEl && sbSpot) {{
                        var t = (spotEl.textContent || '').trim();
                        if (t && t !== sbSpot.textContent) {{
                            sbSpot.textContent = t;
                            if (lastSpot !== null) {{
                                var prev = parseFloat(String(lastSpot).replace(/[^0-9.\\-]/g, ''));
                                var cur = parseFloat(t.replace(/[^0-9.\\-]/g, ''));
                                sbSpot.classList.remove('up', 'down');
                                if (!isNaN(prev) && !isNaN(cur)) {{
                                    sbSpot.classList.add(cur >= prev ? 'up' : 'down');
                                }}
                            }}
                            lastSpot = t;
                            lastSpotChangeAt = Date.now();
                        }}
                    }}
                    if (symEl && sbSym) sbSym.textContent = (symEl.textContent || '').trim().replace(/.*\\(([^)]+)\\).*/, '$1') || symEl.textContent;
                    if (modeRoot && sbMode) {{
                        var checked = modeRoot.querySelector('input[type=radio]:checked');
                        if (checked) sbMode.textContent = (checked.value || '').toUpperCase();
                    }}
                    if (sbClock) sbClock.textContent = fmtTime(new Date());
                    if (sbDot && sbLat) {{
                        var age = (Date.now() - lastSpotChangeAt) / 1000;
                        sbDot.classList.remove('stale', 'dead');
                        if (age > 60) sbDot.classList.add('dead');
                        else if (age > 15) sbDot.classList.add('stale');
                        sbLat.textContent = age < 1 ? '< 1 s' : Math.round(age) + ' s';
                    }}
                }} catch (err) {{ /* swallow */ }}
            }}
            setInterval(tick, 1000);
            document.addEventListener('DOMContentLoaded', tick);
        }})();
        </script>
    </body>
</html>"""

app.layout = html.Div(
    className="app-shell",
    children=[
        dcc.Interval(id="spot-refresh", interval=SPOT_REFRESH_MS, n_intervals=0),
        dcc.Interval(id="chart-refresh", interval=30000, n_intervals=0),
        dcc.Interval(id="alerts-refresh", interval=ALERT_REFRESH_MS, n_intervals=0),
        dcc.Interval(id="health-refresh", interval=HEALTH_REFRESH_MS, n_intervals=0),
        dcc.Interval(id="replay-play", interval=1500, n_intervals=0, disabled=True),
        dcc.Store(id="spot-history", data=[]),
        dcc.Store(id="alert-state", data={"prev_flip": None, "prev_oi_wall": None, "prev_vol_regime": None}),
        dcc.Store(id="positions-store", data=POSITIONS_SEED),
        dcc.Store(id="replay-play-state", data={"playing": False}),
        dcc.Store(id="idea-apply-store", data=[]),
        dcc.Store(id="active-page", data="market"),
        dcc.Store(id="palette-state", data={"open": False}),
        dcc.Store(id="workspace-store", storage_type="local", data={}),
        dcc.Store(id="replay-scale-store", data={}),
        dcc.Store(id="strategy-suite-chain-store", data=[]),
        dcc.Store(id="bt-next-trade-store", data={}),
        dcc.Store(
            id="strategy-suite-builder-store",
            data={"symbol": "BTC", "template": "long_call", "commission": DEFAULT_COMMISSION_PER_CONTRACT, "eval_days": 7, "legs": []},
        ),
        html.Div(
            className="header-bar",
            children=[
                html.Div(
                    className="brand",
                    children=[
                        html.Div(className="brand-dot"),
                        html.Div(
                            [
                                html.P("GEX TERMINAL", className="brand-title"),
                                html.P("Gamma Exposure Analytics", className="brand-subtitle"),
                            ]
                        ),
                    ],
                ),
                html.Div("LIVE", className="status-pill"),
            ],
        ),
        # ── Top Tab Nav ──
        html.Div(
            className="floating-toolbar",
            children=[
                html.Button(
                    id="tb-market",
                    className="tb-btn active",
                    children=[
                        html.Span("\u2191\u2193", className="tb-icon"),
                        html.Span("Market", className="tb-label"),
                        html.Span("1", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
                html.Div(className="toolbar-sep"),
                html.Button(
                    id="tb-strategy",
                    className="tb-btn",
                    children=[
                        html.Span("\u2696", className="tb-icon"),
                        html.Span("Strategy", className="tb-label"),
                        html.Span("2", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
                html.Div(className="toolbar-sep"),
                html.Button(
                    id="tb-alerts",
                    className="tb-btn",
                    children=[
                        html.Span("\u26a0", className="tb-icon"),
                        html.Span("Alerts", className="tb-label"),
                        html.Span("3", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
                html.Div(className="toolbar-sep"),
                html.Button(
                    id="tb-portfolio",
                    className="tb-btn",
                    children=[
                        html.Span("\u25a3", className="tb-icon"),
                        html.Span("Portfolio", className="tb-label"),
                        html.Span("4", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
                html.Div(className="toolbar-sep"),
                html.Button(
                    id="tb-backtest",
                    className="tb-btn",
                    children=[
                        html.Span("\u23f1", className="tb-icon"),
                        html.Span("Backtest", className="tb-label"),
                        html.Span("5", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
                html.Div(className="toolbar-sep"),
                html.Button(
                    id="tb-ops",
                    className="tb-btn",
                    children=[
                        html.Span("\u2699", className="tb-icon"),
                        html.Span("Data Ops", className="tb-label"),
                        html.Span("6", className="tb-key"),
                    ],
                    n_clicks=0,
                ),
            ],
        ),
        html.Div(
            className="toolbar",
            children=[
                html.Div(
                    className="toolbar-grid",
                    children=[
                        html.Div(
                            [
                                html.Div("Asset", className="control-label"),
                                dcc.Dropdown(
                                    id="symbol",
                                    options=[
                                        {"label": "Bitcoin (BTC)", "value": "BTC"},
                                        {"label": "Ethereum (ETH)", "value": "ETH"},
                                    ],
                                    value="BTC",
                                    clearable=False,
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Div("Exchanges", className="control-label"),
                                dcc.Dropdown(
                                    id="exchange-selector",
                                    options=EXCHANGE_FILTER_OPTIONS,
                                    value=EXCHANGE_ORDER,
                                    multi=True,
                                    clearable=False,
                                    placeholder="Filter exchanges",
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Div("Expiration Range", className="control-label"),
                                dcc.DatePickerRange(
                                    id="expiry-range",
                                    display_format="YYYY-MM-DD",
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Div("Expiries", className="control-label"),
                                dcc.Dropdown(
                                    id="expiry-selector",
                                    multi=True,
                                    placeholder="Select one or more expiries",
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Div("Data Mode", className="control-label"),
                                dcc.RadioItems(
                                    id="data-mode",
                                    className="level-filter mode-filter",
                                    inputStyle={"marginRight": "0px"},
                                    labelStyle={"marginRight": "6px", "display": "inline-block"},
                                    labelClassName="label-body",
                                    options=[
                                        {"label": "Live", "value": "live"},
                                        {"label": "Replay", "value": "replay"},
                                    ],
                                    value="live",
                                    inline=True,
                                ),
                            ]
                        ),
                        html.Div(
                            className="control-stack",
                            children=[
                                html.Div("Replay Snapshot", className="control-label"),
                                dcc.Dropdown(
                                    id="replay-timestamp",
                                    placeholder="Select snapshot",
                                    clearable=False,
                                    disabled=False,
                                ),
                                html.Button("Play", id="replay-play-btn", className="action-button secondary small", disabled=True),
                            ]
                        ),
                        html.Div(
                            className="control-stack",
                            children=[
                                html.Div("Workspace", className="control-label"),
                                html.Div(
                                    className="workspace-controls",
                                    children=[
                                        html.Button("Save", id="workspace-save-btn", className="action-button secondary small", n_clicks=0),
                                        html.Button("Load", id="workspace-load-btn", className="action-button secondary small", n_clicks=0),
                                        html.Span(id="workspace-status", className="panel-subtitle"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
        # Command palette (Ctrl+K)
        html.Div(
            id="command-palette",
            className="command-palette command-palette-hidden",
            children=[
                html.Div(
                    className="command-palette-modal",
                    children=[
                        html.Div(
                            className="command-palette-head",
                            children=[
                                dcc.Input(
                                    id="palette-input",
                                    type="text",
                                    placeholder="Type a command \u2026 (Ctrl+K to toggle, Esc to close)",
                                    className="command-palette-input",
                                    autoComplete="off",
                                ),
                                html.Span("ESC", className="command-palette-hint"),
                            ],
                        ),
                        html.Div(
                            className="command-palette-list",
                            id="command-palette-list",
                            children=[
                                html.Div("1 \u00b7 Market Analysis", className="command-palette-item", **{"data-action": "page:market", "data-keys": "market analysis gex chart"}),
                                html.Div("2 \u00b7 Strategy Suite", className="command-palette-item", **{"data-action": "page:strategy", "data-keys": "strategy builder optimizer flow"}),
                                html.Div("3 \u00b7 Alerts & Tickets", className="command-palette-item", **{"data-action": "page:alerts", "data-keys": "alerts tickets ack"}),
                                html.Div("4 \u00b7 Portfolio Risk", className="command-palette-item", **{"data-action": "page:portfolio", "data-keys": "portfolio risk scenario"}),
                                html.Div("5 \u00b7 Data Ops & Replay", className="command-palette-item", **{"data-action": "page:ops", "data-keys": "data ops replay snapshot"}),
                                html.Div("Symbol \u2192 BTC", className="command-palette-item", **{"data-action": "symbol:BTC", "data-keys": "btc bitcoin symbol"}),
                                html.Div("Symbol \u2192 ETH", className="command-palette-item", **{"data-action": "symbol:ETH", "data-keys": "eth ethereum symbol"}),
                                html.Div("Mode \u2192 Live", className="command-palette-item", **{"data-action": "mode:live", "data-keys": "live mode data"}),
                                html.Div("Mode \u2192 Replay", className="command-palette-item", **{"data-action": "mode:replay", "data-keys": "replay mode historical"}),
                                html.Div("Workspace \u2192 Save current", className="command-palette-item", **{"data-action": "workspace:save", "data-keys": "workspace save layout"}),
                                html.Div("Workspace \u2192 Load saved", className="command-palette-item", **{"data-action": "workspace:load", "data-keys": "workspace load layout"}),
                                html.Div("Reload Chain (Deribit)", className="command-palette-item", **{"data-action": "click:strategy-suite-refresh-chain-btn", "data-keys": "reload chain deribit refresh"}),
                            ],
                        ),
                    ],
                ),
            ],
        ),
        html.Div(id="watchlist-row", className="watchlist-row"),
        html.Div(
            className="info-cards",
            children=[
                html.Div([html.Div("Spot Price", className="k"), html.Div(id="spot-card", className="v"), html.Div(id="spot-sparkline", className="info-card-spark")], className="info-card"),
                html.Div([html.Div("Max Pain", className="k"), html.Div(id="max-pain-card", className="v")], className="info-card"),
                html.Div([html.Div("GEX Flip", className="k"), html.Div(id="flip-card", className="v"), html.Div(id="flip-source-note", className="flip-source-note")], className="info-card"),
                html.Div([html.Div("Net GEX", className="k"), html.Div(id="net-card", className="v")], className="info-card"),
                html.Div([html.Div("OI Wall", className="k"), html.Div(id="heat-card", className="v")], className="info-card"),
            ],
        ),
        html.Div(
            className="dashboard-grid",
            children=[
                # ══ PAGE: Market ══
                html.Div(
                    id="page-market",
                    className="page-group page-active",
                    children=[
                html.Div("Market Analysis", className="section-label"),
                html.Div(
                    id="section-chart",
                    className="panel chart-panel",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[html.H3("GEX / Spot Chart", className="panel-title")],
                                ),
                                html.Div(
                                    className="spot-head-controls",
                                    children=[
                                        dcc.Dropdown(
                                            id="chart-view-mode",
                                            className="chart-type-field",
                                            options=[
                                                {"label": "GEX by Strike", "value": "gex"},
                                                {"label": "Spot Candles + Levels", "value": "spot"},
                                            ],
                                            value="gex",
                                            clearable=False,
                                        ),
                                        dcc.Dropdown(
                                            id="gex-metrics",
                                            className="chart-metrics-field",
                                            options=GEX_METRIC_OPTIONS,
                                            value=DEFAULT_GEX_METRICS,
                                            multi=True,
                                            placeholder="Pick GEX metrics to overlay",
                                        ),
                                        html.Div(
                                            id="deribit-timeframe-wrap",
                                            children=dcc.Dropdown(
                                                id="deribit-timeframe",
                                                className="spot-head-field",
                                                options=DERIBIT_TIMEFRAME_OPTIONS,
                                                value=DERIBIT_RESOLUTION,
                                                clearable=False,
                                            ),
                                        ),
                                        html.Div(
                                            id="spot-source-wrap",
                                            style={"display": "none"},
                                            children=dcc.Dropdown(
                                                id="spot-source",
                                                options=[{"label": "Deribit Perp", "value": "deribit"}],
                                                value="deribit",
                                                clearable=False,
                                            ),
                                        ),
                                        html.Div(
                                            id="spot-session-bars-wrap",
                                            children=dcc.Dropdown(
                                                id="spot-session-bars",
                                                className="spot-head-field",
                                                options=SESSION_BAR_OPTIONS,
                                                value=DEFAULT_SESSION_BARS,
                                                clearable=False,
                                            ),
                                        ),
                                        html.Div(
                                            className="replay-inline-controls",
                                            children=[
                                                html.Button(
                                                    "Play",
                                                    id="replay-play-btn-chart",
                                                    className="action-button secondary small",
                                                    disabled=True,
                                                ),
                                                html.Button(
                                                    "Step",
                                                    id="replay-step-btn",
                                                    className="action-button secondary small",
                                                    disabled=True,
                                                ),
                                            ],
                                        ),
                                        html.Span(id="chart-caption", className="spot-head-caption"),
                                    ],
                                ),
                            ],
                        ),
                        html.Iframe(id="main-chart", className="dash-graph", style=_IFRAME_STYLE_TALL),
                    ],
                ),
                html.Div(
                    id="section-dealer-flow",
                    className="panel dealer-flow-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Dealer Hedge Flow", className="panel-title"),
                                        html.Span("Required hedge per spot shock", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(id="dealer-flow-panel"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Net GEX > 0 (Mean-Reverting): dealers SELL into rallies and BUY dips — flows dampen volatility."),
                                                html.Li("Net GEX < 0 (Trend-Following): dealers BUY rallies and SELL dips — flows amplify moves and feed gamma squeezes."),
                                                html.Li("Each card shows the notional dealers must trade after a ±1% / ±2% spot shock to stay delta-neutral. Larger numbers = more reflexive market impact."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-vanna-charm",
                    className="panel vanna-charm-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Vanna / Charm Exposure", className="panel-title"),
                                        html.Span("Dealer dV/dσ and dDelta/dT by strike", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="vanna-charm-chart", className="tool-graph", style=_IFRAME_STYLE),
                                html.Div(id="vanna-charm-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Vanna bars (left axis): how much dealer delta changes per +1 vol-point per +1% spot move. Positive bars = dealers buy as IV rises; negative = dealers sell."),
                                                html.Li("Charm diamonds (right axis): dealer delta decay per calendar day. Big charm near spot is the classic 'pin to strike' driver in the days before expiry."),
                                                html.Li("Dealer view (sign already inverted from raw customer OI). Pulled live from Deribit chain with mark IV per strike."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-intraday-gex",
                    className="panel intraday-gex-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Intraday GEX History", className="panel-title"),
                                        html.Span("Net GEX evolution + spot overlay", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="intraday-gex-chart", className="tool-graph", style=_IFRAME_STYLE),
                                html.Div(id="intraday-gex-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Green line: net dealer gamma exposure across the session (history persisted to gex_snapshots.db)."),
                                                html.Li("Blue line (right axis): spot price evolution. Watch for net GEX flipping sign vs. spot drift to anticipate regime changes."),
                                                html.Li("Empty until the collector has written a few snapshots — the writer is in collector.py."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-vol-surface",
                    className="panel vol-surface-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Vol Surface", className="panel-title"),
                                        html.Span("Implied vol by strike \u00d7 expiry", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="vol-surface-chart", className="tool-graph", style=_IFRAME_STYLE_HM),
                                html.Div(id="vol-surface-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Cooler colors = lower IV, warmer = higher IV. Skew is read down a column; term structure across a row."),
                                                html.Li("White dashed line marks the strike closest to spot \u2014 use it to inspect ATM skew quickly."),
                                                html.Li("Strikes are clipped to \u00b135% from spot for readability. Pulled from Deribit chain mark IV."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-rv-iv",
                    className="panel rv-iv-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Realized vs Implied", className="panel-title"),
                                        html.Span("Parkinson RV \u00b7 ATM IV \u00b7 VRP", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="rv-iv-chart", className="tool-graph", style=_IFRAME_STYLE),
                                html.Div(id="rv-iv-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Amber line: Parkinson realized vol (uses high/low \u2014 more efficient than close-to-close) annualized."),
                                                html.Li("Blue dashed line: ATM implied vol from the live chain."),
                                                html.Li("VRP = IV \u2212 RV. Positive (green) = premium rich, negative (red) = premium cheap. Drives short/long premium decisions."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-hedge-backtest",
                    className="panel hedge-backtest-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Hedge Flow Backtest", className="panel-title"),
                                        html.Span("Regime predictor cumulative hit rate", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="hedge-backtest-chart", className="tool-graph", style=_IFRAME_STYLE),
                                html.Div(id="hedge-backtest-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to read this"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Each snapshot, the regime predictor calls direction: NetGEX > 0 \u2192 mean-revert (\u2212), NetGEX < 0 \u2192 trend (+)."),
                                                html.Li("Green line: cumulative fraction of times the prediction matched the realized next-snapshot move sign."),
                                                html.Li("Above the dashed 50% baseline = the GEX regime is currently informative for short-horizon direction."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-levels",
                    className="panel levels-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Key Levels", className="panel-title"),
                                        html.Span("Support & Resistance", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(
                                    className="levels-toolbar",
                                    children=[
                                        dcc.RadioItems(
                                            id="level-filter",
                                            className="level-filter",
                                            inputStyle={"marginRight": "0px"},
                                            labelStyle={"marginRight": "6px", "display": "inline-block"},
                                            labelClassName="level-filter-label",
                                            options=[
                                                {"label": html.Span("All", className="label-body"), "value": "all"},
                                                {"label": html.Span("Positive", className="label-body"), "value": "positive"},
                                                {"label": html.Span("Negative", className="label-body"), "value": "negative"},
                                                {"label": html.Span("Absolute", className="label-body"), "value": "absolute"},
                                                {"label": html.Span("Pivot", className="label-body"), "value": "pivot"},
                                            ],
                                            value="all",
                                            inline=True,
                                        ),
                                    ],
                                ),
                                html.Div(id="levels-panel"),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="section-ideas",
                    className="panel ideas-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Trade Ideas", className="panel-title"),
                                        html.Span("GEX-Driven Setups", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(className="sidebar-body", children=[html.Div(id="strategy-panel")]),
                    ],
                ),
                html.Div(
                    id="heatmap-panel-container",
                    className="panel heatmap-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Options Heatmap", className="panel-title"),
                                        html.Span("Open Interest by Strike & Expiry", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Iframe(id="options-heatmap-chart", className="tool-graph", style=_IFRAME_STYLE_HM),
                                html.Div(id="heatmap-summary", className="panel-subtitle"),
                                html.Details(
                                    className="tool-guide",
                                    children=[
                                        html.Summary("How to use this chart"),
                                        html.Ul(
                                            className="tool-guide-list",
                                            children=[
                                                html.Li("Each cell is total open interest at one strike and expiry. Brighter cells are larger positioning clusters."),
                                                html.Li("Treat the brightest zone as the current OI wall. Price often reacts near those strikes as dealers hedge inventory."),
                                                html.Li("Compare front expiries vs later expiries: front-heavy heat suggests short-term pin risk; broader heat curve suggests more distributed positioning."),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    className="panel telegram-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Telegram Levels", className="panel-title"),
                                        html.Span("Level Notifications", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(
                                    className="telegram-grid",
                                    children=[
                                        html.Div(
                                            className="telegram-row",
                                            children=[
                                                html.Div("Channel", className="telegram-label"),
                                                (
                                                    html.A(TELEGRAM_CHANNEL_URL, href=TELEGRAM_CHANNEL_URL, target="_blank", className="telegram-link")
                                                    if TELEGRAM_CHANNEL_URL
                                                    else html.Span("not configured", className="telegram-value")
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            className="telegram-row",
                                            children=[
                                                html.Div("Bot", className="telegram-label"),
                                                html.Span(TELEGRAM_BOT_HANDLE or "not configured", className="telegram-value"),
                                            ],
                                        ),
                                        html.Div(
                                            className="telegram-row",
                                            children=[
                                                html.Div("Request", className="telegram-label"),
                                                html.Span("/levels BTC 7d", className="telegram-value"),
                                            ],
                                        ),
                                    ],
                                ),
                                html.Div(
                                    "Requests return the latest SP/MP/GF/P1/P2/N1/N2/A1/A2 levels from options_data.csv. "
                                    "Configure TELEGRAM_* env vars and run scripts/telegram_levels_bot.py to enable.",
                                    className="telegram-note",
                                ),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
                # ══ PAGE: Strategy ══
                html.Div(
                    id="page-strategy",
                    className="page-group",
                    children=[
                html.Div("Strategy Suite", className="section-label"),
                build_strategy_suite_panel(),
                    ],
                ),
                # ══ PAGE: Alerts ══
                html.Div(
                    id="page-alerts",
                    className="page-group",
                    children=[
                html.Div("Alerts & Monitoring", className="section-label"),
                html.Div(
                    id="section-alerts",
                    className="panel alerts-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Alerts & Tickets", className="panel-title"),
                                        html.Span("Live Monitoring", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(
                                    className="alert-controls",
                                    children=[
                                        html.Div(
                                            [
                                                html.Div("Severity", className="control-label"),
                                                dcc.Dropdown(
                                                    id="alert-severity-filter",
                                                    options=[
                                                        {"label": "High", "value": "high"},
                                                        {"label": "Medium", "value": "medium"},
                                                        {"label": "Low", "value": "low"},
                                                    ],
                                                    value=["high", "medium", "low"],
                                                    multi=True,
                                                    clearable=False,
                                                ),
                                            ],
                                            className="control-stack",
                                        ),
                                        html.Div(
                                            [
                                                html.Div("Show", className="control-label"),
                                                dcc.Checklist(
                                                    id="alert-unacked-only",
                                                    options=[{"label": "Unacked Only", "value": "unacked"}],
                                                    value=["unacked"],
                                                ),
                                            ],
                                            className="control-stack",
                                        ),
                                        html.Button("Acknowledge All", id="ack-alerts-btn", className="action-button secondary"),
                                    ],
                                ),
                                html.Div(id="alerts-panel"),
                                html.Div(id="alerts-status", className="panel-subtitle"),
                                html.Div(id="alerts-ack-status", className="panel-subtitle"),
                                html.Div(id="alerts-last-refresh", className="panel-subtitle"),
                                html.Div(
                                    className="alert-controls",
                                    children=[
                                        html.Button("Export Tickets", id="export-tickets-btn", className="action-button"),
                                        html.Div(id="tickets-status", className="panel-subtitle"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
                # ══ PAGE: Portfolio ══
                html.Div(
                    id="page-portfolio",
                    className="page-group",
                    children=[
                html.Div("Portfolio & Risk", className="section-label"),
                html.Div(
                    id="section-portfolio",
                    className="panel portfolio-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Portfolio Risk & Scenario Lab", className="panel-title"),
                                        html.Span("Greeks & Scenario Analysis", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(
                                    className="position-editor",
                                    children=[
                                        html.Div("Position Builder", className="panel-subtitle"),
                                        html.Div(
                                            className="position-controls",
                                            children=[
                                                html.Div(
                                                    [
                                                        html.Div("Symbol", className="control-label"),
                                                        dcc.Dropdown(
                                                            id="position-symbol",
                                                            options=[
                                                                {"label": "BTC", "value": "BTC"},
                                                                {"label": "ETH", "value": "ETH"},
                                                            ],
                                                            value="BTC",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Div("Expiry", className="control-label"),
                                                        dcc.DatePickerSingle(
                                                            id="position-expiry",
                                                            display_format="YYYY-MM-DD",
                                                            date=DEFAULT_POSITION_EXPIRY,
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Div("Strike", className="control-label"),
                                                        dcc.Input(
                                                            id="position-strike",
                                                            type="number",
                                                            placeholder="e.g. 65000",
                                                            className="text-input",
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Div("Type", className="control-label"),
                                                        dcc.Dropdown(
                                                            id="position-type",
                                                            options=[
                                                                {"label": "Call", "value": "call"},
                                                                {"label": "Put", "value": "put"},
                                                            ],
                                                            value="call",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Div("Quantity", className="control-label"),
                                                        dcc.Input(
                                                            id="position-quantity",
                                                            type="number",
                                                            placeholder="e.g. 1",
                                                            className="text-input",
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Div("Avg Price", className="control-label"),
                                                        dcc.Input(
                                                            id="position-avg-price",
                                                            type="number",
                                                            placeholder="e.g. 2500",
                                                            className="text-input",
                                                        ),
                                                    ],
                                                    className="control-stack",
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            className="position-actions",
                                            children=[
                                                html.Button("Add Position", id="add-position-btn", className="action-button"),
                                                html.Button("Save Positions CSV", id="save-positions-btn", className="action-button secondary"),
                                                html.Button("Reload CSV", id="load-positions-btn", className="action-button secondary"),
                                                html.Button("Clear All", id="clear-positions-btn", className="action-button secondary"),
                                            ],
                                        ),
                                        html.Div(id="positions-status", className="panel-subtitle"),
                                        html.Div(id="positions-table", className="mini-table position-table"),
                                    ],
                                ),
                                html.Div(id="portfolio-summary", className="portfolio-cards"),
                                html.Div(id="portfolio-table", className="mini-table"),
                                html.Div(
                                    className="alert-controls",
                                    children=[
                                        html.Div(
                                            [
                                                html.Div("Spot Shift (%)", className="control-label"),
                                                dcc.Slider(id="spot-shift", min=-5, max=5, step=0.25, value=0, marks={-5: "-5", -2: "-2", 0: "0", 2: "+2", 5: "+5"}),
                                            ],
                                            className="control-stack",
                                        ),
                                        html.Div(
                                            [
                                                html.Div("Vol Shift (%)", className="control-label"),
                                                dcc.Slider(id="vol-shift", min=-15, max=15, step=1, value=0, marks={-15: "-15", -5: "-5", 0: "0", 5: "+5", 15: "+15"}),
                                            ],
                                            className="control-stack",
                                        ),
                                    ],
                                ),
                                html.Div(id="scenario-summary", className="panel-subtitle"),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
                # ══ PAGE: Options Backtest ══
                html.Div(
                    id="page-backtest",
                    className="page-group",
                    children=[
                html.Div("Options Selling Backtest", className="section-label"),
                html.Div(
                    id="section-options-backtest",
                    className="panel options-backtest-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Options Selling Backtest", className="panel-title"),
                                        html.Span("Test strategies on BTC & ETH with historical Friday 08:00 UTC settlement", className="panel-subtitle"),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                # Controls row
                                html.Div(
                                    className="backtest-controls",
                                    style={"display": "grid", "gridTemplateColumns": "repeat(7, 1fr)", "gap": "10px", "marginBottom": "12px"},
                                    children=[
                                        html.Div([
                                            html.Div("Asset", className="control-label"),
                                            dcc.Dropdown(
                                                id="bt-symbol",
                                                options=[{"label": "BTC", "value": "BTC"}, {"label": "ETH", "value": "ETH"}],
                                                value="BTC",
                                                clearable=False,
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Strategy", className="control-label"),
                                            dcc.Dropdown(
                                                id="bt-strategy",
                                                options=[
                                                    {"label": "Short Put", "value": "short_put"},
                                                    {"label": "Cash-Secured Put", "value": "cash_secured_put"},
                                                    {"label": "Short Call", "value": "short_call"},
                                                    {"label": "Short Strangle", "value": "short_strangle"},
                                                    {"label": "Iron Condor", "value": "iron_condor"},
                                                    {"label": "Covered Call", "value": "covered_call"},
                                                    {"label": "Covered Put", "value": "covered_put"},
                                                ],
                                                value="short_put",
                                                clearable=False,
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Cycle", className="control-label"),
                                            dcc.Dropdown(
                                                id="bt-cycle",
                                                options=[
                                                    {"label": "Weekly", "value": "weekly"},
                                                    {"label": "Monthly", "value": "monthly"},
                                                ],
                                                value="weekly",
                                                clearable=False,
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Delta", className="control-label"),
                                            dcc.Dropdown(
                                                id="bt-delta",
                                                options=[{"label": f"{d:.0%}", "value": d} for d in DEFAULT_DELTAS],
                                                value=0.15,
                                                clearable=False,
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Lookback (days)", className="control-label"),
                                            dcc.Dropdown(
                                                id="bt-days",
                                                options=[
                                                    {"label": "180d", "value": 180},
                                                    {"label": "365d", "value": 365},
                                                    {"label": "730d (2y)", "value": 730},
                                                ],
                                                value=365,
                                                clearable=False,
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Capital ($)", className="control-label"),
                                            dcc.Input(
                                                id="bt-capital",
                                                type="number",
                                                value=100000,
                                                min=1000,
                                                step=1000,
                                                style={"width": "100%", "background": "#1a1a2e", "color": "#e0e0e0", "border": "1px solid #333", "borderRadius": "4px", "padding": "6px"},
                                            ),
                                        ], className="control-stack"),
                                        html.Div([
                                            html.Div("Reinvest", className="control-label"),
                                            dcc.Checklist(
                                                id="bt-reinvest",
                                                options=[{"label": " Compound premiums", "value": "yes"}],
                                                value=[],
                                                style={"color": "#e0e0e0", "fontSize": "12px", "paddingTop": "6px"},
                                            ),
                                        ], className="control-stack"),
                                    ],
                                ),
                                # Run button
                                html.Div(
                                    style={"marginBottom": "16px"},
                                    children=[
                                        html.Button("Run Backtest", id="bt-run-btn", className="action-button", n_clicks=0),
                                        dcc.Loading(
                                            id="bt-loading",
                                            type="circle",
                                            children=[html.Div(id="bt-status", className="panel-subtitle", style={"marginTop": "8px"})],
                                        ),
                                    ],
                                ),
                                # Stats cards
                                html.Div(id="bt-stats-cards", style={"marginBottom": "16px"}),
                                # Equity curve chart
                                dcc.Graph(
                                    id="bt-equity-chart",
                                    style={"width": "100%", "height": "350px"},
                                    config={"displayModeBar": False},
                                    figure={"data": [], "layout": {"template": "plotly_dark", "paper_bgcolor": "#0d0d1a", "plot_bgcolor": "#0d0d1a", "height": 350, "xaxis": {"visible": False}, "yaxis": {"visible": False}, "annotations": [{"text": "Click 'Run Backtest' to generate equity curve", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False, "font": {"size": 14, "color": "#555"}}]}},
                                ),
                                # PnL per trade chart
                                dcc.Graph(
                                    id="bt-pnl-chart",
                                    style={"width": "100%", "height": "300px", "marginTop": "12px"},
                                    config={"displayModeBar": False},
                                    figure={"data": [], "layout": {"template": "plotly_dark", "paper_bgcolor": "#0d0d1a", "plot_bgcolor": "#0d0d1a", "height": 300, "xaxis": {"visible": False}, "yaxis": {"visible": False}, "annotations": [{"text": "PnL per trade will appear here", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False, "font": {"size": 14, "color": "#555"}}]}},
                                ),
                                # Next trade idea
                                html.Div(
                                    id="bt-next-trade",
                                    style={"marginTop": "16px"},
                                ),
                                # Trade log table
                                html.Div(
                                    id="bt-trade-log",
                                    style={"marginTop": "16px", "maxHeight": "400px", "overflowY": "auto"},
                                ),
                                # ── Strategy Manual ──
                                html.Details(
                                    style={"marginTop": "24px", "borderTop": "1px solid #333", "paddingTop": "16px"},
                                    open=False,
                                    children=[
                                        html.Summary(
                                            "Strategy Manual",
                                            style={"cursor": "pointer", "fontSize": "15px", "fontWeight": "bold", "color": "#c084fc", "marginBottom": "12px"},
                                        ),
                                        html.Div(
                                            style={"fontSize": "12px", "lineHeight": "1.7", "color": "#ccc"},
                                            children=[
                                                # ── General concepts ──
                                                html.H4("Key Concepts", style={"color": "#60a5fa", "margin": "0 0 8px 0", "fontSize": "13px"}),
                                                html.P([
                                                    html.Strong("Delta"), " measures how much an option's price moves per $1 move in the underlying. ",
                                                    "For sellers, delta represents the approximate probability that the option expires in-the-money. ",
                                                    "A ", html.Strong("30\u0394 (0.30)"), " call has roughly a 30% chance of expiring ITM \u2014 or a 70% chance of expiring worthless (profitable for the seller).",
                                                ]),
                                                html.P([
                                                    html.Strong("Settlement:"), " All trades settle at ", html.Strong("Friday 08:00 UTC"),
                                                    " (Deribit expiry convention). Weekly = every Friday; Monthly = last Friday of the month.",
                                                ]),
                                                html.P([
                                                    html.Strong("Reinvest Premiums:"), " When checked, position size scales with your current equity. ",
                                                    "If your account grew 10%, you sell 1.1\u00d7 contracts \u2014 compounding gains (and losses).",
                                                ]),
                                                html.Hr(style={"borderColor": "#333", "margin": "16px 0"}),
                                                # ── Short Put ──
                                                html.H4("Short Put", style={"color": "#4ade80", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "Sell an OTM put at your target delta. You collect premium upfront and profit if the price ",
                                                    "stays above the strike at expiry. ",
                                                    html.Strong("Risk:"), " if the underlying drops below the strike, you lose strike \u2212 settlement (minus the premium cushion).",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 BTC Short Put, 15\u0394, weekly:"),
                                                    html.Br(),
                                                    "BTC = $95,000. You sell the $88,500 put (15\u0394) for ~$420. ",
                                                    "If BTC is above $88,500 on Friday 08:00 UTC, the put expires worthless and you keep the $420. ",
                                                    "If BTC drops to $85,000, you lose $88,500 \u2212 $85,000 = $3,500, offset by the $420 premium \u2192 net loss $3,080.",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #4ade80"}),
                                                # ── Short Call ──
                                                html.H4("Short Call", style={"color": "#f87171", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "Sell an OTM call at your target delta. You collect premium and profit if the price ",
                                                    "stays below the strike. ",
                                                    html.Strong("Risk:"), " theoretically unlimited if the underlying rallies far above the strike.",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 ETH Short Call, 20\u0394, monthly:"),
                                                    html.Br(),
                                                    "ETH = $3,200. You sell the $3,650 call (20\u0394) for ~$85. ",
                                                    "ETH finishes at $3,400 \u2192 call expires worthless, you keep $85. ",
                                                    "ETH rallies to $4,000 \u2192 you owe $4,000 \u2212 $3,650 = $350, minus $85 premium \u2192 net loss $265.",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #f87171"}),
                                                # ── Short Strangle ──
                                                html.H4("Short Strangle", style={"color": "#fbbf24", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "Sell both an OTM put and an OTM call at the same delta. You collect double premium and profit ",
                                                    "as long as price stays between the two strikes. ",
                                                    html.Strong("Risk:"), " loss on either side if the underlying makes a large move.",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 BTC Short Strangle, 25\u0394, weekly:"),
                                                    html.Br(),
                                                    "BTC = $95,000. You sell the $89,000 put (25\u0394) and $101,000 call (25\u0394). ",
                                                    "Combined premium: ~$1,200. BTC settles at $96,500 \u2192 both expire OTM, you keep $1,200. ",
                                                    "If BTC drops to $85,000 \u2192 put costs $89,000 \u2212 $85,000 = $4,000, offset by $1,200 \u2192 net loss $2,800.",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #fbbf24"}),
                                                # ── Iron Condor ──
                                                html.H4("Iron Condor", style={"color": "#c084fc", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "A short strangle with protective wings: sell OTM put + call, then buy a further-OTM put + call (5% of spot away from the short strikes). ",
                                                    "The wings cap your maximum loss but reduce the premium collected. ",
                                                    html.Strong("Max loss"), " = width of the widest spread minus net credit.",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 BTC Iron Condor, 30\u0394, weekly:"),
                                                    html.Br(),
                                                    "BTC = $95,000. Wing width = 5% \u00d7 $95,000 = $4,750.", html.Br(),
                                                    "Sell $87,500 put (30\u0394), buy $82,750 put (wing). Sell $102,500 call (30\u0394), buy $107,250 call (wing).", html.Br(),
                                                    "Net credit: ~$1,800 (short premiums) \u2212 ~$600 (long premiums) = $1,200.", html.Br(),
                                                    "Max loss per spread = $4,750 \u2212 $1,200 = $3,550.", html.Br(),
                                                    "BTC settles at $94,000 \u2192 all legs expire OTM, you keep the $1,200 credit.", html.Br(),
                                                    "BTC drops to $80,000 \u2192 short put costs $87,500 \u2212 $80,000 = $7,500 but long put pays $82,750 \u2212 $80,000 = $2,750. ",
                                                    "Net option payout = \u2212$4,750, plus $1,200 credit \u2192 max loss $3,550.",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #c084fc"}),
                                                # ── Covered Call ──
                                                html.H4("Covered Call", style={"color": "#38bdf8", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "Hold 1 unit of the underlying and sell an OTM call against it. The premium provides income and a small downside cushion, ",
                                                    "but your upside is capped at the strike. ",
                                                    html.Strong("Best when:"), " you expect sideways-to-slightly-bullish price action.",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 BTC Covered Call, 30\u0394, weekly:"),
                                                    html.Br(),
                                                    "BTC = $95,000. You hold 1 BTC and sell the $102,500 call (30\u0394) for ~$900.", html.Br(),
                                                    "BTC settles at $97,000 \u2192 call expires OTM. Underlying PnL = +$2,000, premium = +$900 \u2192 total +$2,900.", html.Br(),
                                                    "BTC rallies to $108,000 \u2192 underlying +$13,000 but call costs $108,000 \u2212 $102,500 = $5,500. ",
                                                    "Total = $13,000 \u2212 $5,500 + $900 = +$8,400 (capped vs. $13,000 without the call).", html.Br(),
                                                    "BTC drops to $90,000 \u2192 underlying \u2212$5,000 + $900 premium \u2192 net \u2212$4,100 (premium softens the loss).",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #38bdf8"}),
                                                # ── Covered Put ──
                                                html.H4("Covered Put", style={"color": "#fb923c", "margin": "0 0 6px 0", "fontSize": "13px"}),
                                                html.P([
                                                    "Short 1 unit of the underlying and sell an OTM put against it. Mirror image of the covered call \u2014 ",
                                                    "you profit from declining prices plus premium, but downside (price rallying against your short) is cushioned only by the premium. ",
                                                    html.Strong("Best when:"), " you expect sideways-to-slightly-bearish price action.",
                                                ]),
                                                html.Div([
                                                    html.Strong("Example \u2014 ETH Covered Put, 25\u0394, monthly:"),
                                                    html.Br(),
                                                    "ETH = $3,200. You short 1 ETH and sell the $2,850 put (25\u0394) for ~$70.", html.Br(),
                                                    "ETH drops to $3,000 \u2192 short underlying +$200, put expires OTM, premium +$70 \u2192 total +$270.", html.Br(),
                                                    "ETH drops to $2,700 \u2192 short underlying +$500, but put costs $2,850 \u2212 $2,700 = $150. Total = $500 \u2212 $150 + $70 = +$420.", html.Br(),
                                                    "ETH rallies to $3,600 \u2192 short underlying \u2212$400 + $70 premium \u2192 net \u2212$330.",
                                                ], style={"background": "#1a1a2e", "borderRadius": "6px", "padding": "10px 12px", "margin": "8px 0 16px 0", "borderLeft": "3px solid #fb923c"}),
                                                # ── Delta guide ──
                                                html.Hr(style={"borderColor": "#333", "margin": "16px 0"}),
                                                html.H4("Choosing Your Delta", style={"color": "#60a5fa", "margin": "0 0 8px 0", "fontSize": "13px"}),
                                                html.Table(
                                                    style={"width": "100%", "borderCollapse": "collapse", "fontSize": "12px", "marginBottom": "12px"},
                                                    children=[
                                                        html.Thead(html.Tr([
                                                            html.Th(c, style={"textAlign": "left", "padding": "6px 10px", "borderBottom": "1px solid #444", "color": "#888"})
                                                            for c in ["Delta", "Approx. OTM %", "Win Prob.", "Premium", "Risk Profile"]
                                                        ])),
                                                        html.Tbody([
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["5\u0394", "~15-20%", "~95%", "Very low", "Very safe, minimal income"]]),
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["10\u0394", "~10-15%", "~90%", "Low", "Conservative income"]]),
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["15\u0394", "~7-12%", "~85%", "Moderate", "Balanced risk/reward"]]),
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["20\u0394", "~5-9%", "~80%", "Good", "Higher income, more exposure"]]),
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["25\u0394", "~4-7%", "~75%", "High", "Aggressive income"]]),
                                                            html.Tr([html.Td(c, style={"padding": "5px 10px", "borderBottom": "1px solid #222"}) for c in
                                                                ["30\u0394", "~3-5%", "~70%", "Very high", "Aggressive, frequent losses"]]),
                                                        ]),
                                                    ],
                                                ),
                                                html.P([
                                                    html.Strong("Tip:"), " In crypto, volatility is high, so even a 10\u0394 option can get breached by sudden moves. ",
                                                    "Use the backtest to compare different deltas over historical data before committing real capital.",
                                                ], style={"color": "#888", "fontStyle": "italic"}),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
                # ══ PAGE: Data Ops ══
                html.Div(
                    id="page-ops",
                    className="page-group",
                    children=[
                html.Div("Data & Operations", className="section-label"),
                html.Div(
                    id="section-ops",
                    className="panel ops-panel-shell",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("Data Ops & Replay", className="panel-title"),
                                        html.Span("System Health & Export", className="panel-subtitle"),
                                    ],
                                ),
                                html.Button("\u25bc", className="panel-collapse-btn"),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body",
                            children=[
                                html.Div(id="data-health-panel"),
                                html.Div(
                                    className="replay-controls",
                                    children=[
                                        html.Div(
                                            [
                                                html.Div("Snapshot Range", className="control-label"),
                                                dcc.DatePickerRange(id="replay-range", display_format="YYYY-MM-DD"),
                                            ],
                                            className="control-stack",
                                        ),
                                        html.Div(id="replay-status", className="panel-subtitle"),
                                        html.Button("Export Snapshot CSV", id="export-snapshot-btn", className="action-button secondary"),
                                        html.Div(id="export-snapshot-status", className="panel-subtitle"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
                # ══ PAGE: Manual ══
                html.Div(
                    id="page-manual",
                    className="page-group",
                    children=[
                html.Div("User Manual", className="section-label"),
                html.Div(
                    className="panel manual-panel",
                    children=[
                        html.Div(
                            className="panel-head",
                            children=[
                                html.Div(
                                    className="panel-head-copy",
                                    children=[
                                        html.H3("GEX Terminal Manual", className="panel-title"),
                                        html.Span("Reference Guide for All Dashboard Tools", className="panel-subtitle"),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="sidebar-body manual-body",
                            children=[
                                # ── Quick Start ──
                                html.Div(className="manual-section", children=[
                                    html.Div("Quick Start", className="manual-heading"),
                                    html.P(
                                        "GEX Terminal is a real-time Gamma Exposure analytics dashboard for crypto options. "
                                        "It aggregates options data from Deribit, Bybit, Binance, and OKX to compute gamma exposure (GEX) "
                                        "levels, key support/resistance zones, and trade ideas.",
                                        className="manual-text",
                                    ),
                                    html.Div(className="manual-keys-grid", children=[
                                        html.Div(className="manual-key-item", children=[
                                            html.Span("1", className="manual-key"), html.Span("Market Analysis", className="manual-key-desc"),
                                        ]),
                                        html.Div(className="manual-key-item", children=[
                                            html.Span("2", className="manual-key"), html.Span("Strategy Suite", className="manual-key-desc"),
                                        ]),
                                        html.Div(className="manual-key-item", children=[
                                            html.Span("3", className="manual-key"), html.Span("Alerts & Tickets", className="manual-key-desc"),
                                        ]),
                                        html.Div(className="manual-key-item", children=[
                                            html.Span("4", className="manual-key"), html.Span("Portfolio Risk", className="manual-key-desc"),
                                        ]),
                                        html.Div(className="manual-key-item", children=[
                                            html.Span("5", className="manual-key"), html.Span("Data Ops", className="manual-key-desc"),
                                        ]),
                                    ]),
                                    html.P(
                                        "Press keys 1\u20135 to switch pages, or click the nav buttons in the header.",
                                        className="manual-text",
                                    ),
                                ]),
                                # ── 1. Market Analysis ──
                                html.Div(className="manual-section", children=[
                                    html.Div([html.Span("1", className="manual-badge"), " Market Analysis"], className="manual-heading"),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("GEX by Strike Chart", className="manual-subheading"),
                                        html.P(
                                            "Displays net gamma exposure per strike aggregated across selected exchanges. "
                                            "Positive GEX (green bars) indicates dealer long gamma \u2014 these strikes act as stabilizers and potential support/resistance. "
                                            "Negative GEX (red bars) indicates dealer short gamma \u2014 these zones amplify moves and increase volatility.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Toggle between GEX by Strike and Spot Candles + Levels view using the dropdown"),
                                            html.Li("Overlay Absolute GEX (AG) as a secondary axis to see total dealer positioning"),
                                            html.Li("Use the metrics dropdown to toggle Net GEX, Call GEX, Put GEX individually"),
                                            html.Li("Chart auto-refreshes every 30 seconds with latest data"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Spot Candles + Levels", className="manual-subheading"),
                                        html.P(
                                            "Shows Deribit perpetual futures OHLC candles with key GEX levels overlaid as horizontal lines. "
                                            "Use this view to see where current price sits relative to computed support/resistance.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Timeframe: select from 1m to 1D candle resolution"),
                                            html.Li("Session bars: choose how many candles to display (50\u2013500)"),
                                            html.Li("Right-side labels show level name and price value"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Key Levels Panel", className="manual-subheading"),
                                        html.P("Computed support/resistance levels from the options chain:", className="manual-text"),
                                        html.Div(className="manual-levels-grid", children=[
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("SP", className="manual-level-code sp"), html.Span("Spot Price \u2014 current market price from the latest data refresh", className="manual-text"),
                                            ]),
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("MP", className="manual-level-code mp"), html.Span("Max Pain \u2014 strike where aggregate option losses are minimal; price tends to pin here near expiry", className="manual-text"),
                                            ]),
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("GF", className="manual-level-code gf"), html.Span("Gamma Flip \u2014 strike where net GEX crosses zero; above = dealer long gamma (mean-reverting), below = short gamma (trending)", className="manual-text"),
                                            ]),
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("P1/P2", className="manual-level-code pos"), html.Span("Positive Gamma \u2014 strikes with highest positive GEX; act as support floors and resistance ceilings (stabilization zones)", className="manual-text"),
                                            ]),
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("N1/N2", className="manual-level-code neg"), html.Span("Negative Gamma \u2014 strikes with most negative GEX; price accelerates through these zones, higher volatility risk", className="manual-text"),
                                            ]),
                                            html.Div(className="manual-level-row", children=[
                                                html.Span("A1/A2", className="manual-level-code abs"), html.Span("Absolute GEX \u2014 strikes with largest total dealer positioning; these are high-attention magnet levels", className="manual-text"),
                                            ]),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Trade Ideas", className="manual-subheading"),
                                        html.P(
                                            "Auto-generated directional and volatility trade setups derived from the current GEX landscape. "
                                            "Each idea includes a confidence score, risk assessment, suggested option structure, and recommended hedge. "
                                            "Click 'Apply to Suite' to load a trade idea directly into the Strategy Builder \u2014 the dashboard "
                                            "automatically navigates to the Strategy page so you can fine-tune the legs immediately.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Dealer Hedge Flow", className="manual-subheading"),
                                        html.P(
                                            "Estimates the notional dealers must trade to stay delta-neutral on a spot shock, derived from "
                                            "Net GEX. Header stats show the current regime (Mean-Reverting if Net GEX > 0, Trend-Following if < 0), "
                                            "Net GEX in dollars, and the spot used. Four shock cards (\u00b11%, \u00b12%) display the required hedge "
                                            "size in USD and underlying units, with a buy/sell tag from the dealer perspective.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Mean-Reverting: dealers sell into rallies and buy into dips \u2014 suppresses volatility"),
                                            html.Li("Trend-Following: dealers buy rallies and sell dips \u2014 amplifies moves"),
                                            html.Li("Hedge formula: hedge_$ \u2248 NetGEX \u00d7 (dS / S), sign-flipped to dealer side"),
                                            html.Li("Works in both live and replay mode (only needs aggregated GEX)"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Vanna / Charm Exposure", className="manual-subheading"),
                                        html.P(
                                            "Per-strike higher-order Greeks shown on a dual-axis chart: vanna bars (left axis) measure dealer "
                                            "P/L sensitivity per +1 vol-point per +1% spot move; charm diamonds (right axis) measure delta decay "
                                            "in USD per calendar day. Sign convention is dealer-side (customer exposures inverted).",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Sourced live from the Deribit option chain (uses per-strike mark_iv)"),
                                            html.Li("Top 20 strikes by combined |vanna| + |charm| magnitude are displayed"),
                                            html.Li("Falls back to an empty state in replay mode \u2014 historical snapshots don't carry per-strike IV"),
                                            html.Li("Use vanna to anticipate hedge flow when IV shifts; charm shows passive delta drift into expiry"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Intraday GEX History", className="manual-subheading"),
                                        html.P(
                                            "Time series of Net GEX through the session with the spot price overlaid on the right axis. "
                                            "Reads from the persistent snapshot store (gex_snapshots.db) which is written by the collector each refresh.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Watch for sign changes in Net GEX vs. spot drift to anticipate regime flips before the static panels show them"),
                                            html.Li("Empty until the collector has run \u2014 it bootstraps once snapshots accumulate"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Vol Surface", className="manual-subheading"),
                                        html.P(
                                            "Heatmap of implied vol across strikes (rows) and expiries (columns). Cooler = lower IV, warmer = higher IV. "
                                            "Skew is read down a column; term structure across a row. White dashed line marks the strike closest to spot.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Strikes clipped to \u00b135% from spot for readability"),
                                            html.Li("Use it to spot rich/cheap wings vs. ATM and unusual term-structure inversions"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Realized vs Implied", className="manual-subheading"),
                                        html.P(
                                            "Rolling Parkinson realized volatility (uses high/low \u2014 more efficient than close-to-close) "
                                            "compared with the live ATM implied vol. Vol Risk Premium = IV \u2212 RV; positive = premium rich, negative = cheap.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Drives short-premium vs. long-premium decisions: rich VRP favors selling, cheap VRP favors buying"),
                                            html.Li("RV is annualized using the median candle resolution \u2014 works on any timeframe"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Hedge Flow Backtest", className="manual-subheading"),
                                        html.P(
                                            "Walk-forward validation of the dealer regime predictor: NetGEX > 0 \u2192 expect mean-reverting next move; "
                                            "NetGEX < 0 \u2192 expect trend continuation. The line shows cumulative hit rate vs. the actual next-snapshot direction.",
                                            className="manual-text",
                                        ),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Above the dashed 50% line = the GEX regime is currently informative for short-horizon direction"),
                                            html.Li("Needs a few snapshots in gex_snapshots.db to be meaningful"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Watchlist Row", className="manual-subheading"),
                                        html.P(
                                            "Multi-symbol mini-tiles above the info cards. Each tile shows symbol, spot, NetGEX, gamma flip, and a "
                                            "regime tag (MR = Mean-Reverting, TF = Trend-Following). Click any tile to jump the dashboard to that symbol.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Command Palette (Ctrl+K)", className="manual-subheading"),
                                        html.P(
                                            "Press Ctrl+K (or \u2318+K on macOS) anywhere in the dashboard to open a fuzzy command palette. "
                                            "Type to filter, \u2191/\u2193 to navigate, Enter to execute, Esc to dismiss. Includes page jumps, symbol switches, mode toggles, and workspace actions.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Workspaces", className="manual-subheading"),
                                        html.P(
                                            "Save / Load buttons in the toolbar persist your current symbol, exchanges, expiries, data mode, and active page "
                                            "to browser localStorage. Use this to jump between different desks (scalper / swing / hedger) without re-typing filters.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Options Heatmap", className="manual-subheading"),
                                        html.P(
                                            "2D heatmap of open interest across strikes (y-axis) and expiries (x-axis). "
                                            "Brighter cells represent larger positioning clusters. Use this to identify OI walls, pin risk zones, "
                                            "and where the majority of dealer inventory is concentrated.",
                                            className="manual-text",
                                        ),
                                    ]),
                                ]),
                                # ── 2. Strategy Suite ──
                                html.Div(className="manual-section", children=[
                                    html.Div([html.Span("2", className="manual-badge"), " Strategy Suite"], className="manual-heading"),
                                    html.P(
                                        "Full-featured options strategy builder and analyzer. Design multi-leg strategies, "
                                        "view payoff diagrams, run scenario analysis, and optimize entry parameters.",
                                        className="manual-text",
                                    ),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Builder Tab", className="manual-subheading"),
                                        html.Ul(className="manual-list", children=[
                                            html.Li("Select a template (Long Call, Bull Spread, Iron Condor, etc.) or build from scratch"),
                                            html.Li("Configure up to 4 legs: type, strike, expiry, quantity, and premium"),
                                            html.Li("Live chain data from Deribit refreshes available strikes and expiries"),
                                            html.Li("Metrics panel shows max profit, max loss, breakevens, and Greeks"),
                                            html.Li("Payoff chart shows P&L at expiry and at the evaluation date"),
                                            html.Li("Scenario table shows P&L under spot shifts (\u00b15%) and vol shifts (\u00b115%)"),
                                        ]),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Optimizer Tab", className="manual-subheading"),
                                        html.P(
                                            "Scans the live option chain to find optimal strike selections for a given strategy template. "
                                            "Ranks candidates by risk/reward ratio, expected value, and Greeks exposure.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-subsection", children=[
                                        html.Div("Flow Tab", className="manual-subheading"),
                                        html.P(
                                            "Analyzes recent option flow to identify large trades, unusual activity, and institutional positioning. "
                                            "Flow data is aggregated from the Deribit trades feed.",
                                            className="manual-text",
                                        ),
                                    ]),
                                ]),
                                # ── 3. Alerts ──
                                html.Div(className="manual-section", children=[
                                    html.Div([html.Span("3", className="manual-badge"), " Alerts & Tickets"], className="manual-heading"),
                                    html.P(
                                        "Automated monitoring system that generates alerts when key market conditions change. "
                                        "Alerts are generated based on GEX level changes, OI wall shifts, and volatility regime transitions.",
                                        className="manual-text",
                                    ),
                                    html.Ul(className="manual-list", children=[
                                        html.Li("High severity: major GEX flip crossover, OI wall migration > 5%"),
                                        html.Li("Medium severity: notable shift in P1/P2 levels, vol regime change"),
                                        html.Li("Low severity: minor level adjustments, data refresh confirmations"),
                                        html.Li("Filter by severity and ack/unack status; export tickets to CSV"),
                                    ]),
                                ]),
                                # ── 4. Portfolio ──
                                html.Div(className="manual-section", children=[
                                    html.Div([html.Span("4", className="manual-badge"), " Portfolio Risk & Scenario Lab"], className="manual-heading"),
                                    html.P(
                                        "Track open option positions and analyze aggregate portfolio Greeks. "
                                        "Run what-if scenarios by shifting spot price and implied volatility.",
                                        className="manual-text",
                                    ),
                                    html.Ul(className="manual-list", children=[
                                        html.Li("Add positions manually or load from CSV (symbol, expiry, strike, type, qty, avg price)"),
                                        html.Li("Portfolio summary shows aggregate Delta, Gamma, Theta, Vega, and total P&L"),
                                        html.Li("Spot shift slider: model \u00b15% spot moves to see Greek sensitivity"),
                                        html.Li("Vol shift slider: model \u00b115% IV changes to see vega impact"),
                                        html.Li("Save positions to CSV for persistence across sessions"),
                                    ]),
                                ]),
                                # ── 5. Data Ops ──
                                html.Div(className="manual-section", children=[
                                    html.Div([html.Span("5", className="manual-badge"), " Data Ops & Replay"], className="manual-heading"),
                                    html.P(
                                        "System health monitoring and historical data management. Track data freshness, "
                                        "export snapshots, and replay historical market states.",
                                        className="manual-text",
                                    ),
                                    html.Ul(className="manual-list", children=[
                                        html.Li("Data health panel shows last refresh time, record counts, and exchange connectivity"),
                                        html.Li("Replay mode: switch to historical snapshots and step through past market states"),
                                        html.Li("Export snapshot CSV: save current computed levels and GEX data for archival"),
                                    ]),
                                ]),
                                # ── GEX Formula ──
                                html.Div(className="manual-section", children=[
                                    html.Div("GEX Calculation Reference", className="manual-heading"),
                                    html.Div(className="manual-formula-box", children=[
                                        html.Div("GEX = \u0393 \u00d7 OI \u00d7 Spot\u00b2 \u00d7 0.01", className="manual-formula"),
                                        html.P(
                                            "Where \u0393 (gamma) is the option's gamma, OI is open interest, and Spot is the underlying price. "
                                            "Call GEX is positive (dealers long gamma), Put GEX is negative when dealers are short puts. "
                                            "Net GEX = Call GEX + Put GEX at each strike.",
                                            className="manual-text",
                                        ),
                                    ]),
                                    html.Div(className="manual-formula-box", children=[
                                        html.Div("Dealer Gamma Regimes", className="manual-formula-label"),
                                        html.P(
                                            "When Net GEX > 0 (above Gamma Flip): dealers are long gamma and hedge by selling rallies / buying dips. "
                                            "This creates a mean-reverting, low-volatility environment. Price tends to pin near high-GEX strikes.",
                                            className="manual-text",
                                        ),
                                        html.P(
                                            "When Net GEX < 0 (below Gamma Flip): dealers are short gamma and must hedge in the same direction as price. "
                                            "This amplifies moves and creates trending, high-volatility conditions.",
                                            className="manual-text",
                                        ),
                                    ]),
                                ]),
                                # ── Data Sources ──
                                html.Div(className="manual-section", children=[
                                    html.Div("Data Sources & Refresh", className="manual-heading"),
                                    html.Ul(className="manual-list", children=[
                                        html.Li("Options data: Deribit, Bybit, Binance, OKX \u2014 collected by gex_engine.py in parallel"),
                                        html.Li("Spot candles: Deribit BTC/ETH perpetual futures OHLC via public API"),
                                        html.Li("Option chain (Strategy Suite): Deribit instruments + ticker endpoint"),
                                        html.Li("Data refresh cycle: options CSV updated by collector, dashboard polls every 30s"),
                                        html.Li("Spot price: updated every 5s from latest candle data"),
                                    ]),
                                ]),
                                # ── Telegram Bot ──
                                html.Div(className="manual-section", children=[
                                    html.Div("Telegram Bot", className="manual-heading"),
                                    html.P(
                                        "An optional Telegram bot can broadcast GEX levels and charts to a channel. "
                                        "Configure TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID in your environment, then run:",
                                        className="manual-text",
                                    ),
                                    html.Div("python scripts/telegram_levels_bot.py", className="manual-code"),
                                    html.P("Commands: /levels BTC weekly | /levels ETH monthly | /levels BTC all", className="manual-text"),
                                ]),
                            ],
                        ),
                    ],
                ),
                    ],
                ),
            ],
        ),
        # ── Footer status bar ──
        html.Div(
            className="status-bar",
            children=[
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Div(id="status-bar-conn-dot", className="status-bar-dot"),
                        html.Span("CONN", className="status-bar-label"),
                        html.Span("DERIBIT", id="status-bar-conn", className="status-bar-value"),
                    ],
                ),
                html.Div(className="status-bar-sep"),
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Span("SYM", className="status-bar-label"),
                        html.Span("BTC", id="status-bar-symbol", className="status-bar-value"),
                    ],
                ),
                html.Div(className="status-bar-sep"),
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Span("SPOT", className="status-bar-label"),
                        html.Span("--", id="status-bar-spot", className="status-bar-value"),
                    ],
                ),
                html.Div(className="status-bar-sep"),
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Span("MODE", className="status-bar-label"),
                        html.Span("LIVE", id="status-bar-mode", className="status-bar-value"),
                    ],
                ),
                html.Div(className="status-bar-spacer"),
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Span("LAT", className="status-bar-label"),
                        html.Span("-- ms", id="status-bar-latency", className="status-bar-value"),
                    ],
                ),
                html.Div(className="status-bar-sep"),
                html.Div(
                    className="status-bar-section",
                    children=[
                        html.Span("UPD", className="status-bar-label"),
                        html.Span("--:--:--", id="status-bar-clock", className="status-bar-value"),
                    ],
                ),
            ],
        ),
    ],
)

# ── Page navigation: toolbar buttons + keyboard shortcuts ──
_PAGE_IDS = ["market", "strategy", "alerts", "portfolio", "backtest", "ops"]
_PAGE_BTN_MAP = {f"tb-{p}": p for p in _PAGE_IDS}

# Single clientside callback handles all 5 buttons → sets active page
app.clientside_callback(
    """
    function(n_market, n_strategy, n_alerts, n_portfolio, n_backtest, n_ops, current) {
        var ctx = window.dash_clientside.callback_context;
        if (!ctx.triggered || ctx.triggered.length === 0) return current;
        var tid = ctx.triggered[0].prop_id.split('.')[0];
        var map = {
            'tb-market': 'market', 'tb-strategy': 'strategy',
            'tb-alerts': 'alerts', 'tb-portfolio': 'portfolio',
            'tb-backtest': 'backtest', 'tb-ops': 'ops'
        };
        return map[tid] || current;
    }
    """,
    Output("active-page", "data"),
    [Input(f"tb-{p}", "n_clicks") for p in _PAGE_IDS],
    State("active-page", "data"),
    prevent_initial_call=True,
)

# Show/hide page groups + highlight active toolbar button
app.clientside_callback(
    """
    function(activePage) {
        var pages = ['market', 'strategy', 'alerts', 'portfolio', 'backtest', 'ops'];
        pages.forEach(function(p) {
            var pg = document.getElementById('page-' + p);
            if (pg) {
                if (p === activePage) {
                    pg.classList.add('page-active');
                    pg.classList.remove('page-hidden');
                } else {
                    pg.classList.remove('page-active');
                    pg.classList.add('page-hidden');
                }
            }
            var btn = document.getElementById('tb-' + p);
            if (btn) {
                if (p === activePage) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            }
        });
        window.scrollTo({top: 0});
        // Attach keyboard shortcuts once
        if (!window._gexKeysAttached) {
            window._gexKeysAttached = true;

            function setPaletteOpen(open) {
                var pal = document.getElementById('command-palette');
                var inp = document.getElementById('palette-input');
                if (!pal) return;
                if (open) {
                    pal.classList.remove('command-palette-hidden');
                    if (inp) { inp.value = ''; setTimeout(function(){ inp.focus(); }, 30); }
                    document.querySelectorAll('.command-palette-item').forEach(function(el){
                        el.classList.remove('palette-hidden');
                        el.classList.remove('palette-highlight');
                    });
                    var first = document.querySelector('.command-palette-item:not(.palette-hidden)');
                    if (first) first.classList.add('palette-highlight');
                } else {
                    pal.classList.add('command-palette-hidden');
                }
                window._paletteOpen = open;
            }
            window._setPaletteOpen = setPaletteOpen;

            function paletteFilter(query) {
                var q = (query || '').toLowerCase().trim();
                var first = null;
                document.querySelectorAll('.command-palette-item').forEach(function(el){
                    var keys = (el.getAttribute('data-keys') || '').toLowerCase();
                    var label = (el.textContent || '').toLowerCase();
                    var visible = !q || keys.indexOf(q) >= 0 || label.indexOf(q) >= 0;
                    if (visible) { el.classList.remove('palette-hidden'); if (!first) first = el; }
                    else { el.classList.add('palette-hidden'); }
                    el.classList.remove('palette-highlight');
                });
                if (first) first.classList.add('palette-highlight');
            }

            function paletteHighlightMove(delta) {
                var items = Array.prototype.slice.call(document.querySelectorAll('.command-palette-item:not(.palette-hidden)'));
                if (!items.length) return;
                var idx = items.findIndex(function(el){ return el.classList.contains('palette-highlight'); });
                idx = (idx < 0 ? 0 : idx + delta);
                if (idx < 0) idx = items.length - 1;
                if (idx >= items.length) idx = 0;
                items.forEach(function(el){ el.classList.remove('palette-highlight'); });
                items[idx].classList.add('palette-highlight');
                items[idx].scrollIntoView({block: 'nearest'});
            }

            function executePaletteAction(action) {
                if (!action) return;
                var parts = action.split(':');
                var kind = parts[0]; var arg = parts.slice(1).join(':');
                if (kind === 'page') {
                    var btn = document.getElementById('tb-' + arg);
                    if (btn) btn.click();
                } else if (kind === 'symbol') {
                    var sym = document.getElementById('symbol');
                    if (sym) {
                        // Dash dropdown: simulate value change via the inner input
                        var ev = new Event('change', {bubbles: true});
                        var input = sym.querySelector('input');
                        if (input) { input.value = arg; }
                        // Best effort: dispatch click on matching option after focus
                        sym.click();
                    }
                } else if (kind === 'mode') {
                    var radios = document.querySelectorAll('#data-mode input[type="radio"]');
                    radios.forEach(function(r){ if (r.value === arg) { r.click(); } });
                } else if (kind === 'click') {
                    var target = document.getElementById(arg);
                    if (target) target.click();
                } else if (kind === 'workspace') {
                    var wbtn = document.getElementById('workspace-' + arg + '-btn');
                    if (wbtn) wbtn.click();
                }
                setPaletteOpen(false);
            }
            window._executePaletteAction = executePaletteAction;

            document.addEventListener('input', function(e) {
                if (e.target && e.target.id === 'palette-input') {
                    paletteFilter(e.target.value);
                }
            });
            document.addEventListener('click', function(e) {
                var item = e.target.closest && e.target.closest('.command-palette-item');
                if (item) {
                    executePaletteAction(item.getAttribute('data-action'));
                    return;
                }
                var pal = document.getElementById('command-palette');
                if (pal && !pal.classList.contains('command-palette-hidden')) {
                    var modal = e.target.closest && e.target.closest('.command-palette-modal');
                    if (!modal) setPaletteOpen(false);
                }
            });

            document.addEventListener('keydown', function(e) {
                // Ctrl+K / Cmd+K toggles palette regardless of focus
                if ((e.ctrlKey || e.metaKey) && (e.key === 'k' || e.key === 'K')) {
                    e.preventDefault();
                    setPaletteOpen(!window._paletteOpen);
                    return;
                }
                if (window._paletteOpen) {
                    if (e.key === 'Escape') { e.preventDefault(); setPaletteOpen(false); return; }
                    if (e.key === 'ArrowDown') { e.preventDefault(); paletteHighlightMove(1); return; }
                    if (e.key === 'ArrowUp')   { e.preventDefault(); paletteHighlightMove(-1); return; }
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        var hi = document.querySelector('.command-palette-item.palette-highlight');
                        if (hi) executePaletteAction(hi.getAttribute('data-action'));
                        return;
                    }
                    return;
                }
                var tag = e.target.tagName.toLowerCase();
                if (tag === 'input' || tag === 'textarea' || tag === 'select') return;
                if (e.target.isContentEditable) return;
                var map = {'1': 'market', '2': 'strategy', '3': 'alerts', '4': 'portfolio', '5': 'backtest', '6': 'ops'};
                var page = map[e.key];
                if (page) {
                    e.preventDefault();
                    var btn = document.getElementById('tb-' + page);
                    if (btn) btn.click();
                }
            });
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("tb-market", "className"),
    Input("active-page", "data"),
)


@app.callback(
    Output("strategy-suite-chain-store", "data"),
    Output("strategy-suite-chain-note", "children"),
    Input("symbol", "value"),
    Input("strategy-suite-refresh-chain-btn", "n_clicks"),
)
def refresh_strategy_suite_chain(symbol, _refresh_clicks):
    symbol = str(symbol or "BTC").upper()
    ctx = dash.callback_context
    trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else ""
    cached_df = CHAIN_CACHE.get(symbol)
    if trigger != "strategy-suite-refresh-chain-btn" and cached_df is not None and not cached_df.empty:
        spot = get_chain_spot(cached_df, symbol)
        expiry_count = len(list_expiries(cached_df))
        return option_chain_store_data(cached_df), f"{symbol} spot {fmt_money(spot, 0)} · {len(cached_df):,} quotes · {expiry_count} expiries (cached)"
    chain_df = fetch_deribit_option_chain(symbol)
    if chain_df is None or chain_df.empty:
        if cached_df is not None and not cached_df.empty:
            spot = get_chain_spot(cached_df, symbol)
            expiry_count = len(list_expiries(cached_df))
            return option_chain_store_data(cached_df), f"{symbol} chain unavailable — using last cached data."
        return [], f"{symbol} chain unavailable from Deribit right now."
    CHAIN_CACHE.set(symbol, chain_df)
    spot = get_chain_spot(chain_df, symbol)
    expiry_count = len(list_expiries(chain_df))
    return option_chain_store_data(chain_df), f"{symbol} spot {fmt_money(spot, 0)} · {len(chain_df):,} quotes · {expiry_count} expiries"


@app.callback(
    Output("strategy-suite-builder-store", "data"),
    Output("strategy-suite-builder-status", "children"),
    Output("strategy-suite-builder-saved", "options"),
    Output("strategy-suite-builder-saved", "value"),
    Input("symbol", "value"),
    Input("strategy-suite-chain-store", "data"),
    Input("strategy-suite-builder-load-btn", "n_clicks"),
    Input("strategy-suite-builder-save-btn", "n_clicks"),
    Input("strategy-suite-builder-delete-btn", "n_clicks"),
    Input("strategy-suite-builder-refresh-btn", "n_clicks"),
    State("strategy-suite-builder-store", "data"),
    State("strategy-suite-builder-template", "value"),
    State("strategy-suite-builder-saved", "value"),
    State("strategy-suite-builder-save-name", "value"),
    State("strategy-suite-builder-commission", "value"),
    State("strategy-suite-builder-eval-days", "value"),
    State({"type": "suite-leg-enabled", "index": ALL}, "value"),
    State({"type": "suite-leg-action", "index": ALL}, "value"),
    State({"type": "suite-leg-type", "index": ALL}, "value"),
    State({"type": "suite-leg-expiry", "index": ALL}, "value"),
    State({"type": "suite-leg-strike", "index": ALL}, "value"),
    State({"type": "suite-leg-qty", "index": ALL}, "value"),
)
def manage_strategy_suite_builder(
    symbol,
    chain_data,
    _load_clicks,
    _save_clicks,
    _delete_clicks,
    _refresh_clicks,
    current_state,
    template_value,
    selected_saved,
    save_name,
    commission_value,
    eval_days_value,
    enabled_values,
    actions,
    leg_types,
    expiries,
    strikes,
    quantities,
):
    ctx = dash.callback_context
    trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else ""
    symbol = str(symbol or "BTC").upper()
    chain_df = option_chain_from_store(chain_data)
    template_value = str(template_value or "long_call")
    builder_state = dict(current_state or {})
    saved_options = strategy_suite_saved_options(symbol)
    valid_saved_values = {item["value"] for item in saved_options}
    selected_saved = selected_saved if selected_saved in valid_saved_values else None
    commission_value = float(pd.to_numeric(commission_value, errors="coerce")) if pd.notna(pd.to_numeric(commission_value, errors="coerce")) else float(DEFAULT_COMMISSION_PER_CONTRACT)
    eval_days_value = float(pd.to_numeric(eval_days_value, errors="coerce")) if pd.notna(pd.to_numeric(eval_days_value, errors="coerce")) else 7.0

    def _seed_state(template_id, name_text=""):
        legs = default_builder_legs(template_id, chain_df, symbol) if chain_df is not None and not chain_df.empty else blank_strategy_suite_legs()
        return {
            "symbol": symbol,
            "template": template_id,
            "commission": commission_value,
            "eval_days": eval_days_value,
            "name": name_text,
            "legs": legs,
        }

    if not builder_state:
        builder_state = _seed_state(template_value)

    if trigger == "strategy-suite-builder-load-btn":
        if not selected_saved:
            return builder_state, "Pick a saved strategy first.", saved_options, selected_saved
        saved_item = next((item for item in strategy_suite_saved_entries(symbol) if item.get("name") == selected_saved), None)
        if not saved_item:
            return builder_state, "Saved strategy not found.", saved_options, None
        builder_state = {
            "symbol": symbol,
            "template": str(saved_item.get("template") or "custom"),
            "commission": float(saved_item.get("commission") or DEFAULT_COMMISSION_PER_CONTRACT),
            "eval_days": float(saved_item.get("eval_days") or 7),
            "name": str(saved_item.get("name") or ""),
            "legs": normalize_builder_legs(saved_item.get("legs") or []),
        }
        return builder_state, f"Loaded saved strategy '{selected_saved}'.", saved_options, selected_saved

    if trigger == "strategy-suite-builder-save-btn":
        strategy_name = str(save_name or selected_saved or "").strip()
        if not strategy_name:
            return builder_state, "Add a save name first.", saved_options, selected_saved
        current_legs = collect_strategy_suite_legs(enabled_values, actions, leg_types, expiries, strikes, quantities)
        builder_state = {
            "symbol": symbol,
            "template": template_value,
            "commission": commission_value,
            "eval_days": eval_days_value,
            "name": strategy_name,
            "legs": current_legs,
        }
        save_strategy(STRATEGY_SUITE_SAVE_PATH, builder_state)
        saved_options = strategy_suite_saved_options(symbol)
        return builder_state, f"Saved '{strategy_name}'.", saved_options, strategy_name

    if trigger == "strategy-suite-builder-delete-btn":
        strategy_name = str(selected_saved or save_name or "").strip()
        if not strategy_name:
            return builder_state, "Choose a saved strategy to delete.", saved_options, selected_saved
        delete_strategy(STRATEGY_SUITE_SAVE_PATH, strategy_name, symbol=symbol)
        saved_options = strategy_suite_saved_options(symbol)
        if str(builder_state.get("name") or "").strip() == strategy_name:
            builder_state["name"] = ""
        return builder_state, f"Deleted '{strategy_name}'.", saved_options, None

    if trigger == "strategy-suite-builder-refresh-btn":
        builder_state = _seed_state(template_value)
        if chain_df is None or chain_df.empty:
            return builder_state, f"Waiting for live {symbol} option chain...", saved_options, None
        return builder_state, f"Reseeded {symbol} legs from current chain.", saved_options, None

    symbol_changed = str(builder_state.get("symbol") or "").upper() != symbol
    missing_legs = not builder_state.get("legs")
    if symbol_changed or missing_legs:
        builder_state = _seed_state(template_value)
        if chain_df is None or chain_df.empty:
            return builder_state, f"Waiting for live {symbol} option chain...", saved_options, None
        return builder_state, f"Loaded live {symbol} chain into the builder.", saved_options, None

    # Fall-through (chain refresh, symbol unchanged, no explicit action):
    # capture the user's in-form leg edits and persist them into builder_state
    # so that any subsequent re-render does NOT wipe their work.
    in_form_legs = collect_strategy_suite_legs(enabled_values, actions, leg_types, expiries, strikes, quantities)
    if in_form_legs:
        builder_state = dict(builder_state)
        builder_state["legs"] = in_form_legs
        builder_state["commission"] = commission_value
        builder_state["eval_days"] = eval_days_value

    if trigger == "strategy-suite-chain-store":
        if chain_df is None or chain_df.empty:
            return builder_state, f"{symbol} chain unavailable right now; keeping current builder setup.", saved_options, selected_saved
        return builder_state, f"Refreshed live {symbol} chain; builder legs preserved.", saved_options, selected_saved

    return builder_state, "", saved_options, selected_saved


@app.callback(
    Output("strategy-suite-builder-template", "value"),
    Output("strategy-suite-builder-save-name", "value"),
    Output("strategy-suite-builder-commission", "value"),
    Output("strategy-suite-builder-eval-days", "value"),
    Input("strategy-suite-builder-store", "data"),
)
def sync_strategy_suite_builder_controls(builder_state):
    builder_state = dict(builder_state or {})
    return (
        str(builder_state.get("template") or "long_call"),
        str(builder_state.get("name") or ""),
        float(builder_state.get("commission") or DEFAULT_COMMISSION_PER_CONTRACT),
        float(builder_state.get("eval_days") or 7),
    )


@app.callback(
    Output("strategy-suite-builder-store", "data", allow_duplicate=True),
    Input("strategy-suite-builder-template", "value"),
    State("strategy-suite-chain-store", "data"),
    State("strategy-suite-builder-store", "data"),
    prevent_initial_call=True,
)
def on_template_select(template_value, chain_data, current_state):
    template_value = str(template_value or "long_call")
    builder_state = dict(current_state or {})
    if str(builder_state.get("template") or "") == template_value and builder_state.get("legs"):
        raise dash.exceptions.PreventUpdate
    chain_df = option_chain_from_store(chain_data)
    symbol = str(builder_state.get("symbol") or "BTC").upper()
    commission_value = float(builder_state.get("commission") or DEFAULT_COMMISSION_PER_CONTRACT)
    eval_days_value = float(builder_state.get("eval_days") or 7)
    legs = (
        default_builder_legs(template_value, chain_df, symbol)
        if chain_df is not None and not chain_df.empty
        else blank_strategy_suite_legs()
    )
    builder_state.update({
        "template": template_value,
        "legs": legs,
        "commission": commission_value,
        "eval_days": eval_days_value,
    })
    return builder_state


@app.callback(
    Output("strategy-suite-builder-legs", "children"),
    Input("strategy-suite-builder-store", "data"),
    State("strategy-suite-chain-store", "data"),
)
def render_strategy_suite_leg_rows(builder_state, chain_data):
    # Re-render only when builder_state changes (load/save/template/refresh/symbol).
    # Chain refreshes are intentionally NOT a trigger here — the strike option
    # lists are kept fresh by `update_strategy_suite_leg_controls` separately,
    # and re-rendering on every chain tick would wipe in-flight user edits.
    chain_df = option_chain_from_store(chain_data)
    return build_strategy_suite_leg_rows(builder_state, chain_df)


@app.callback(
    Output({"type": "suite-leg-strike", "index": ALL}, "options"),
    Output({"type": "suite-leg-strike", "index": ALL}, "value"),
    Output({"type": "suite-leg-strike", "index": ALL}, "disabled"),
    Output({"type": "suite-leg-expiry", "index": ALL}, "disabled"),
    Input({"type": "suite-leg-type", "index": ALL}, "value"),
    Input({"type": "suite-leg-expiry", "index": ALL}, "value"),
    Input("strategy-suite-chain-store", "data"),
    State({"type": "suite-leg-strike", "index": ALL}, "value"),
)
def update_strategy_suite_leg_controls(leg_types, expiries, chain_data, current_strikes):
    chain_df = option_chain_from_store(chain_data)
    if not leg_types:
        return [], [], [], []
    symbol = str(chain_df["symbol"].iloc[0]).upper() if chain_df is not None and not chain_df.empty and "symbol" in chain_df.columns else "BTC"
    spot = get_chain_spot(chain_df, symbol)
    strike_options = []
    strike_values = []
    strike_disabled = []
    expiry_disabled = []
    for idx, leg_type in enumerate(leg_types):
        leg_type = str(leg_type or "call")
        expiry_value = (expiries or [None])[idx] if idx < len(expiries or []) else None
        current_strike = (current_strikes or [None])[idx] if idx < len(current_strikes or []) else None
        if leg_type == "spot":
            strike_options.append([])
            strike_values.append(None)
            strike_disabled.append(True)
            expiry_disabled.append(True)
            continue
        strikes = list_strikes(chain_df, expiry_value, leg_type)
        strike_options.append([{"label": fmt_price(value), "value": float(value)} for value in strikes])
        current_num = pd.to_numeric(current_strike, errors="coerce")
        if pd.notna(current_num) and float(current_num) in strikes:
            strike_values.append(float(current_num))
        elif strikes:
            target = float(current_num) if pd.notna(current_num) else spot
            strike_values.append(nearest_strike(strikes, target) if target is not None else None)
        else:
            strike_values.append(None)
        strike_disabled.append(False)
        expiry_disabled.append(False)
    return strike_options, strike_values, strike_disabled, expiry_disabled


@app.callback(
    Output("strategy-suite-builder-summary", "children"),
    Output("strategy-suite-builder-metrics", "children"),
    Output("strategy-suite-builder-chart", "srcDoc"),
    Output("strategy-suite-builder-scenarios", "children"),
    Input("symbol", "value"),
    Input("strategy-suite-builder-template", "value"),
    Input("strategy-suite-chain-store", "data"),
    Input("strategy-suite-builder-commission", "value"),
    Input("strategy-suite-builder-eval-days", "value"),
    Input({"type": "suite-leg-enabled", "index": ALL}, "value"),
    Input({"type": "suite-leg-action", "index": ALL}, "value"),
    Input({"type": "suite-leg-type", "index": ALL}, "value"),
    Input({"type": "suite-leg-expiry", "index": ALL}, "value"),
    Input({"type": "suite-leg-strike", "index": ALL}, "value"),
    Input({"type": "suite-leg-qty", "index": ALL}, "value"),
)
def update_strategy_suite_builder_report(
    symbol,
    template_value,
    chain_data,
    commission_value,
    eval_days_value,
    enabled_values,
    actions,
    leg_types,
    expiries,
    strikes,
    quantities,
):
    chain_df = option_chain_from_store(chain_data)
    commission_value = float(pd.to_numeric(commission_value, errors="coerce")) if pd.notna(pd.to_numeric(commission_value, errors="coerce")) else float(DEFAULT_COMMISSION_PER_CONTRACT)
    eval_days_value = float(pd.to_numeric(eval_days_value, errors="coerce")) if pd.notna(pd.to_numeric(eval_days_value, errors="coerce")) else 7.0
    legs = collect_strategy_suite_legs(enabled_values, actions, leg_types, expiries, strikes, quantities)
    report = evaluate_strategy(chain_df, symbol, legs, commission_per_contract=commission_value, eval_days=eval_days_value)
    title_text = f"{template_label(template_value)} Payoff" if template_value else "Strategy Payoff"
    return (
        build_strategy_suite_summary(report),
        build_strategy_suite_metrics(report),
        build_strategy_suite_payoff_figure(report, title_text),
        build_strategy_suite_scenarios(report),
    )


@app.callback(
    Output("strategy-suite-optimizer-status", "children"),
    Output("strategy-suite-optimizer-summary", "children"),
    Output("strategy-suite-optimizer-chart", "srcDoc"),
    Output("strategy-suite-optimizer-table", "children"),
    Input("symbol", "value"),
    Input("strategy-suite-chain-store", "data"),
    Input("strategy-suite-optimizer-bias", "value"),
    Input("strategy-suite-optimizer-objective", "value"),
    Input("strategy-suite-optimizer-max-cost", "value"),
    Input("strategy-suite-optimizer-min-pop", "value"),
    Input("strategy-suite-optimizer-eval-days", "value"),
    Input("strategy-suite-optimizer-run-btn", "n_clicks"),
)
def update_strategy_suite_optimizer(
    symbol,
    chain_data,
    bias,
    objective,
    max_cost_pct,
    min_pop,
    eval_days_value,
    _run_clicks,
):
    chain_df = option_chain_from_store(chain_data)
    if chain_df is None or chain_df.empty:
        empty = build_empty_figure("Load a live BTC or ETH chain to run the optimizer.")
        return "Waiting for live option chain...", html.Div("No optimizer results yet.", className="suite-empty"), empty, html.Div("No candidates yet.", className="suite-empty")
    max_cost_pct = float(pd.to_numeric(max_cost_pct, errors="coerce")) if pd.notna(pd.to_numeric(max_cost_pct, errors="coerce")) else 0.20
    min_pop = float(pd.to_numeric(min_pop, errors="coerce")) if pd.notna(pd.to_numeric(min_pop, errors="coerce")) else 0.0
    eval_days_value = float(pd.to_numeric(eval_days_value, errors="coerce")) if pd.notna(pd.to_numeric(eval_days_value, errors="coerce")) else 7.0
    candidates = build_optimizer_candidates(
        chain_df,
        symbol,
        bias,
        objective=objective,
        eval_days=eval_days_value,
        max_cost_pct=max_cost_pct,
        min_pop=min_pop,
    )
    if not candidates:
        empty = build_empty_figure("No optimizer candidates matched these filters.")
        return "No candidates matched the current filters.", html.Div("Try widening cost or POP thresholds.", className="suite-empty"), empty, html.Div("No candidates yet.", className="suite-empty")
    status = f"Scored {len(candidates)} candidates for {str(symbol or 'BTC').upper()}."
    return (
        status,
        build_strategy_suite_optimizer_summary(candidates),
        build_strategy_suite_optimizer_figure(candidates),
        build_strategy_suite_optimizer_table(candidates),
    )


@app.callback(
    Output("strategy-suite-flow-status", "children"),
    Output("strategy-suite-flow-summary", "children"),
    Output("strategy-suite-flow-chart", "srcDoc"),
    Output("strategy-suite-flow-table", "children"),
    Input("symbol", "value"),
    Input("strategy-suite-flow-count", "value"),
    Input("strategy-suite-flow-min-premium", "value"),
    Input("strategy-suite-flow-type", "value"),
    Input("strategy-suite-flow-direction", "value"),
    Input("strategy-suite-flow-refresh-btn", "n_clicks"),
)
def update_strategy_suite_flow(symbol, trade_count, min_premium, option_type, direction, _refresh_clicks):
    symbol = str(symbol or "BTC").upper()
    trade_count_num = pd.to_numeric(trade_count, errors="coerce")
    trade_count = int(max(10, min(int(trade_count_num) if pd.notna(trade_count_num) else 60, 150)))
    min_premium = float(pd.to_numeric(min_premium, errors="coerce")) if pd.notna(pd.to_numeric(min_premium, errors="coerce")) else 0.0
    flow_df = fetch_deribit_options_flow(symbol, count=trade_count)
    if flow_df is None or flow_df.empty:
        empty = build_empty_figure(f"No recent {symbol} flow is available right now.")
        return f"{symbol} flow unavailable from Deribit.", html.Div("No flow summary yet.", className="suite-empty"), empty, html.Div("No flow rows yet.", className="suite-empty")
    if option_type in {"call", "put"}:
        flow_df = flow_df[flow_df["type"] == option_type].copy()
    if direction in {"buy", "sell"}:
        flow_df = flow_df[flow_df["direction"] == direction].copy()
    flow_df["premium_usd"] = pd.to_numeric(flow_df["premium_usd"], errors="coerce").fillna(0.0)
    if min_premium > 0:
        flow_df = flow_df[flow_df["premium_usd"] >= min_premium].copy()
    if flow_df.empty:
        empty = build_empty_figure("No trades matched the current flow filters.")
        return "No recent trades matched the current flow filters.", html.Div("Try lowering the premium threshold.", className="suite-empty"), empty, html.Div("No flow rows yet.", className="suite-empty")
    status = f"{len(flow_df):,} recent {symbol} option trades after filters."
    return status, build_strategy_suite_flow_summary(flow_df), build_strategy_suite_flow_figure(flow_df), build_strategy_suite_flow_table(flow_df)


@app.callback(
    Output("expiry-selector", "options"),
    Output("expiry-selector", "value"),
    Input("symbol", "value"),
    Input("exchange-selector", "value"),
    Input("expiry-range", "start_date"),
    Input("expiry-range", "end_date"),
    Input("data-mode", "value"),
    Input("replay-timestamp", "value"),
    State("expiry-selector", "value"),
)
def update_expiry_options(symbol, selected_exchanges, start_date, end_date, data_mode, replay_timestamp, current_values):
    data = get_latest_data()
    if data_mode == "replay" and replay_timestamp:
        snapshot = load_snapshot_at(SNAPSHOT_DB, symbol, replay_timestamp)
        if snapshot is not None and not snapshot.empty:
            snapshot = snapshot.copy()
            snapshot["exchange"] = "Snapshot"
            data = snapshot
            selected_exchanges = ["Snapshot"]
    if "exchange" in data.columns:
        selected_exchanges = normalize_exchange_selection(selected_exchanges, data)
        dff = data[(data["symbol"] == symbol) & (data["exchange"].isin(selected_exchanges))]
    else:
        dff = data[data["symbol"] == symbol]
    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        dff = dff[(dff["expiry"] >= start) & (dff["expiry"] <= end)]
    expiries = sorted(dff["expiry"].unique())
    options = [{"label": e.strftime("%Y-%m-%d"), "value": e.strftime("%Y-%m-%d")} for e in expiries]
    option_values = [item["value"] for item in options]

    # Preserve user-selected expiries when still available.
    kept_values = [value for value in (current_values or []) if value in option_values]
    if kept_values:
        return options, kept_values

    # First-load default: auto-select only this week's expiries (next 7 days).
    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    week_end = today + pd.Timedelta(days=7)
    default_values = [item["value"] for item, expiry in zip(options, expiries) if today <= pd.Timestamp(expiry).normalize() <= week_end]
    if default_values:
        return options, default_values

    # Fallback when no near-term expiries are available.
    return options, option_values


# gamma_flip_level lives in pro/signals.py — re-exported here so the existing
# call sites in this module keep working without an `import as` rename.
from pro.signals import gamma_flip_level  # noqa: E402


@app.callback(
    Output("spot-history", "data"),
    Input("spot-refresh", "n_intervals"),
    Input("symbol", "value"),
    Input("exchange-selector", "value"),
    State("spot-history", "data"),
)
def update_spot_history(_n_intervals, symbol, selected_exchanges, history):
    data = get_latest_data()
    selected_exchanges = normalize_exchange_selection(selected_exchanges, data)
    history = history or []
    asset_rows = data[(data["symbol"] == symbol) & (data["exchange"].isin(selected_exchanges))]
    if asset_rows.empty:
        return history
    exchange_sig = ",".join(selected_exchanges)
    point = {
        "t": pd.Timestamp.now("UTC").isoformat(),
        "p": float(asset_rows["spot_price"].iloc[-1]),
        "s": symbol,
        "e": exchange_sig,
    }
    if history and (history[-1].get("s") != symbol or history[-1].get("e") != exchange_sig):
        history = []
    history.append(point)
    if len(history) > SPOT_HISTORY_LIMIT:
        history = history[-SPOT_HISTORY_LIMIT:]
    return history


@app.callback(
    Output("deribit-timeframe-wrap", "style"),
    Output("spot-session-bars-wrap", "style"),
    Output("spot-source-wrap", "style"),
    Output("heatmap-panel-container", "style"),
    Input("chart-view-mode", "value"),
)
def toggle_spot_controls(chart_view_mode):
    hidden = {"display": "none"}
    if chart_view_mode == "spot":
        return {"display": "block"}, {"display": "block"}, hidden, hidden
    return hidden, hidden, hidden, {}


@app.callback(
    Output("spot-sparkline", "children"),
    Input("spot-history", "data"),
)
def update_spot_sparkline(history):
    if not history or len(history) < 2:
        return None
    prices = [point["p"] for point in history[-80:]]
    return build_sparkline_graph(prices)



@app.callback(
    Output("main-chart", "srcDoc"),
    Output("levels-panel", "children"),
    Output("spot-card", "children"),
    Output("max-pain-card", "children"),
    Output("flip-card", "children"),
    Output("flip-source-note", "children"),
    Output("net-card", "children"),
    Output("heat-card", "children"),
    Output("strategy-panel", "children"),
    Output("chart-caption", "children"),
    Output("options-heatmap-chart", "srcDoc"),
    Output("heatmap-summary", "children"),
    Output("idea-apply-store", "data"),
    Input("symbol", "value"),
    Input("exchange-selector", "value"),
    Input("expiry-selector", "value"),
    Input("chart-refresh", "n_intervals"),
    Input("deribit-timeframe", "value"),
    Input("spot-session-bars", "value"),
    Input("spot-source", "value"),
    Input("chart-view-mode", "value"),
    Input("gex-metrics", "value"),
    Input("level-filter", "value"),
    Input("data-mode", "value"),
    Input("replay-timestamp", "value"),
    Input("replay-range", "start_date"),
    Input("replay-range", "end_date"),
    Input("replay-scale-store", "data"),
    State("spot-history", "data"),
)
def update_gex_chart(
    symbol,
    selected_exchanges,
    selected_expiries,
    _chart_intervals,
    deribit_timeframe,
    spot_session_bars,
    spot_source,
    chart_view_mode,
    gex_metrics,
    level_filter,
    data_mode,
    replay_timestamp,
    replay_start,
    replay_end,
    replay_scale,
    spot_history,
):
    use_replay = data_mode == "replay" and bool(replay_timestamp)
    data = get_latest_data()
    exchange_caption = ", ".join(selected_exchanges or [])
    if use_replay:
        snapshot = load_snapshot_at(SNAPSHOT_DB, symbol, replay_timestamp)
        if snapshot is None or snapshot.empty:
            data = pd.DataFrame()
        else:
            snapshot = snapshot.copy()
            snapshot["exchange"] = "Snapshot"
            snapshot["call_oi"] = 0.0
            snapshot["put_oi"] = 0.0
            data = snapshot
        selected_exchanges = ["Snapshot"]
        exchange_caption = "Snapshot"
    else:
        selected_exchanges = normalize_exchange_selection(selected_exchanges, data)
        exchange_caption = ", ".join(selected_exchanges)

    timeframe = deribit_timeframe or DERIBIT_RESOLUTION
    if spot_session_bars == "all":
        session_bars = None
    else:
        session_bars = int(spot_session_bars or DEFAULT_SESSION_BARS)
    view_mode = chart_view_mode or "gex"
    selected_metric_values = gex_metrics or DEFAULT_GEX_METRICS
    available_metric_values = {"net_gex", "ag", "call_gex", "put_gex"}
    selected_metric_values = [m for m in selected_metric_values if m in available_metric_values]
    if not selected_metric_values:
        selected_metric_values = ["net_gex"]
    empty_heat_fig, empty_heat_summary, empty_heat_card = build_option_heatmap_tool(pd.DataFrame())
    mode_label = "Replay" if use_replay else "Live"
    spot_source = spot_source or "spot"
    bars_label = "all" if session_bars is None else f"{session_bars} bars"
    if view_mode == "spot" and use_replay and spot_source == "spot":
        spot_caption = f"{mode_label} | Snapshot spot | {bars_label} | GEX: {exchange_caption}"
    elif view_mode == "spot" and use_replay and spot_source == "deribit":
        spot_caption = (
            f"{mode_label} | Deribit {'BTC-PERPETUAL' if symbol == 'BTC' else 'ETH-PERPETUAL'} | "
            f"{timeframe_label(timeframe)} | {bars_label} | GEX: {exchange_caption}"
        )
    elif view_mode == "spot" and spot_source == "spot":
        spot_caption = f"{mode_label} | Spot (options) | {bars_label} | GEX: {exchange_caption}"
    else:
        spot_caption = (
            f"{mode_label} | Deribit {'BTC-PERPETUAL' if symbol == 'BTC' else 'ETH-PERPETUAL'} | "
            f"{timeframe_label(timeframe)} | {bars_label} | GEX: {exchange_caption}"
        )

    replay_history = None
    if use_replay and view_mode == "spot" and spot_source == "spot":
        start_ts, end_ts = resolve_replay_window(replay_start, replay_end)
        replay_end_ts = pd.to_datetime(replay_timestamp, utc=True) if replay_timestamp else end_ts
        hist_df = load_snapshot_range(SNAPSHOT_DB, symbol, start_ts, replay_end_ts)
        if hist_df is not None and not hist_df.empty:
            spot_series = (
                hist_df.groupby("ts_utc", as_index=False)["spot_price"]
                .median()
                .sort_values("ts_utc")
            )
            if session_bars is not None and len(spot_series) > SPOT_HISTORY_LIMIT:
                spot_series = spot_series.tail(SPOT_HISTORY_LIMIT)
            replay_history = [{"t": row["ts_utc"], "p": float(row["spot_price"])} for _, row in spot_series.iterrows()]
        else:
            replay_history = []

    use_spot_source = view_mode == "spot" and spot_source == "spot"
    fill_spot_gaps = bool(replay_history is not None) or (use_spot_source and not use_replay)
    smooth_window = 3 if fill_spot_gaps else 0
    spot_series_points = replay_history if replay_history is not None else (spot_history or [])
    fixed_range = None
    if use_replay and view_mode == "spot" and isinstance(replay_scale, dict):
        if replay_scale.get("min") is not None and replay_scale.get("max") is not None:
            fixed_range = (replay_scale.get("min"), replay_scale.get("max"))

    if data is None or data.empty:
        empty_gex = build_empty_figure()
        empty_spot = build_spot_figure(
            spot_series_points,
            {"spot": None},
            max_bars=session_bars,
            asset_symbol=symbol,
            fill_gaps=fill_spot_gaps,
            smooth_window=smooth_window,
            fixed_range=fixed_range,
        )
        selected_fig = empty_spot if view_mode == "spot" else empty_gex
        metric_caption = ", ".join(GEX_METRIC_LABELS.get(m, m) for m in selected_metric_values)
        selected_caption = spot_caption if view_mode == "spot" else f"GEX by Strike | {metric_caption} | {exchange_caption} | No data"
        return (
            selected_fig,
            html.Div("No levels available", className="panel-subtitle"),
            "n/a",
            "n/a",
            "n/a",
            "source: n/a",
            "n/a",
            empty_heat_card,
            html.Div("No strategy ideas available", className="panel-subtitle"),
            selected_caption,
            empty_heat_fig,
            empty_heat_summary,
        )

    if not selected_expiries:
        empty_gex = build_empty_figure()
        empty_spot = build_spot_figure(
            spot_series_points,
            {"spot": None},
            max_bars=session_bars,
            asset_symbol=symbol,
            fill_gaps=fill_spot_gaps,
            smooth_window=smooth_window,
            fixed_range=fixed_range,
        )
        selected_fig = empty_spot if view_mode == "spot" else empty_gex
        metric_caption = ", ".join(GEX_METRIC_LABELS.get(m, m) for m in selected_metric_values)
        selected_caption = spot_caption if view_mode == "spot" else f"GEX by Strike | {metric_caption} | {exchange_caption} | Select expiries"
        return (
            selected_fig,
            html.Div("No levels available", className="panel-subtitle"),
            "n/a",
            "n/a",
            "n/a",
            "source: n/a",
            "n/a",
            empty_heat_card,
            html.Div("Select expiries to generate option strategy ideas", className="panel-subtitle"),
            selected_caption,
            empty_heat_fig,
            empty_heat_summary,
        )

    selected_expiries = pd.to_datetime(selected_expiries)
    dff = data[
        (data["symbol"] == symbol)
        & (data["exchange"].isin(selected_exchanges))
        & (data["expiry"].isin(selected_expiries))
    ]
    if dff.empty:
        empty_gex = build_empty_figure()
        empty_spot = build_spot_figure(
            spot_series_points,
            {"spot": None},
            max_bars=session_bars,
            asset_symbol=symbol,
            fill_gaps=fill_spot_gaps,
            smooth_window=smooth_window,
            fixed_range=fixed_range,
        )
        selected_fig = empty_spot if view_mode == "spot" else empty_gex
        metric_caption = ", ".join(GEX_METRIC_LABELS.get(m, m) for m in selected_metric_values)
        selected_caption = spot_caption if view_mode == "spot" else f"GEX by Strike | {metric_caption} | {exchange_caption} | No rows"
        return (
            selected_fig,
            html.Div("No levels available", className="panel-subtitle"),
            "n/a",
            "n/a",
            "n/a",
            "source: n/a",
            "n/a",
            empty_heat_card,
            html.Div("No strategy ideas available for this filter", className="panel-subtitle"),
            selected_caption,
            empty_heat_fig,
            empty_heat_summary,
        )

    dff = dff.assign(
        total_gex=dff["call_gex"] + dff["put_gex"],
        abs_gex=dff["call_gex"].abs() + dff["put_gex"].abs(),
    )
    heat_fig, heat_summary, heat_card = build_option_heatmap_tool(dff)
    exchange_names = ordered_exchange_list(dff["exchange"].dropna().tolist())
    gex_by_strike = dff.groupby("strike", as_index=False)[["total_gex", "abs_gex", "call_gex", "put_gex"]].sum()

    spot_price = float(dff["spot_price"].iloc[-1])
    mp_strike = float(gex_by_strike.loc[gex_by_strike["total_gex"].abs().idxmin(), "strike"])
    pos_gamma = gex_by_strike[gex_by_strike["total_gex"] > 0].nlargest(2, "total_gex")["strike"].values
    neg_gamma = gex_by_strike[gex_by_strike["total_gex"] < 0].nsmallest(2, "total_gex")["strike"].values
    a_levels = gex_by_strike.nlargest(2, "abs_gex")["strike"].values
    flip_value = gamma_flip_level(gex_by_strike)
    flip_strike = float(flip_value) if flip_value is not None and pd.notna(flip_value) else mp_strike
    net_total = float(gex_by_strike["total_gex"].sum())
    if not use_replay:
        record_net_gex(symbol, net_total)
        write_metric(
            SNAPSHOT_DB,
            symbol=symbol,
            net_gex=net_total,
            row_count=len(dff),
            unique_strikes=int(dff["strike"].nunique()),
        )

    # --- Build GEX Bokeh figure ---
    use_ag = "ag" in selected_metric_values

    # Compute smart bar width from median gap between sorted unique strikes
    sorted_strikes = sorted(gex_by_strike["strike"].unique().tolist())
    active_exchanges = [e for e in exchange_names if not dff[dff["exchange"] == e].empty]
    n_active = max(len(active_exchanges), 1)
    if len(sorted_strikes) >= 2:
        gaps = [sorted_strikes[i + 1] - sorted_strikes[i] for i in range(len(sorted_strikes) - 1)]
        median_gap = sorted(gaps)[len(gaps) // 2]
        bar_width = median_gap * 0.72 / n_active
    else:
        bar_width = max((gex_by_strike["strike"].max() - gex_by_strike["strike"].min()) * 0.01, 50.0)

    gex_p = bokeh_figure(
        sizing_mode="stretch_both",
        x_axis_label="Strike", y_axis_label="Net GEX (k)",
        tools="pan,wheel_zoom,box_zoom,reset,hover,crosshair",
        active_scroll="wheel_zoom",
    )
    if use_ag:
        gex_p.extra_y_ranges = {"ag": Range1d(start=0, end=1)}
        ag_axis = LinearAxis(
            y_range_name="ag", axis_label="AG (M)",
            formatter=NumeralTickFormatter(format="0,0"),
        )
        gex_p.add_layout(ag_axis, "right")

    src_hover = ColumnDataSource(dict(
        strike=gex_by_strike["strike"].tolist(),
        net_gex=(gex_by_strike["total_gex"] / NET_GEX_SCALE).tolist(),
        call_gex=(gex_by_strike["call_gex"] / NET_GEX_SCALE).tolist(),
        put_gex=(gex_by_strike["put_gex"] / NET_GEX_SCALE).tolist(),
    ))

    if "net_gex" in selected_metric_values:
        render_idx = 0
        for idx, exchange_name in enumerate(exchange_names):
            exchange_df = (
                dff[dff["exchange"] == exchange_name]
                .groupby("strike", as_index=False)[["total_gex"]]
                .sum()
            )
            if exchange_df.empty:
                continue
            bar_color = exchange_color(exchange_name, idx)
            # Offset bars per exchange when multiple are shown (dodge)
            x_offset = (render_idx - (n_active - 1) / 2.0) * bar_width if n_active > 1 else 0.0
            render_idx += 1
            src_bar = ColumnDataSource(dict(
                x=(exchange_df["strike"] + x_offset).tolist(),
                top=(exchange_df["total_gex"] / NET_GEX_SCALE).tolist(),
                strike=exchange_df["strike"].tolist(),
                gex=(exchange_df["total_gex"] / NET_GEX_SCALE).tolist(),
            ))
            gex_p.vbar(
                x="x", top="top", source=src_bar,
                width=bar_width * 0.92,
                fill_color=bar_color, line_color="#0d1821",
                line_alpha=0.3, fill_alpha=0.86,
                legend_label=f"Net GEX ({exchange_name})",
            )

    if "call_gex" in selected_metric_values:
        gex_p.line("strike", "call_gex", source=src_hover,
                   color="#4ec88c", line_width=2.2, legend_label="Call GEX")

    if "put_gex" in selected_metric_values:
        gex_p.line("strike", "put_gex", source=src_hover,
                   color="#ff7f8d", line_width=2.2, legend_label="Put GEX")

    if use_ag:
        ag_vals_series = gex_by_strike["abs_gex"] / AG_SCALE
        ag_max = float(ag_vals_series.max()) if not ag_vals_series.empty else 1.0
        gex_p.extra_y_ranges["ag"] = Range1d(start=0, end=ag_max * 1.15 if ag_max > 0 else 1)
        src_ag = ColumnDataSource(dict(
            x=gex_by_strike["strike"].tolist(),
            y=ag_vals_series.tolist(),
            y0=[0.0] * len(ag_vals_series),
        ))
        gex_p.line("x", "y", source=src_ag, color="#eaecf0", line_width=2,
                   y_range_name="ag", legend_label="AG (Absolute GEX)")
        gex_p.scatter("x", "y", source=src_ag, size=4, color="#eaecf0",
                      y_range_name="ag")
        # varea supports y_range_name (unlike Band)
        gex_p.varea("x", y1="y0", y2="y", source=src_ag,
                    fill_alpha=0.14, fill_color="#f0aa4d", y_range_name="ag")

    gex_p.add_layout(Span(location=0, dimension="width",
                           line_color="#8a92a6", line_alpha=0.22, line_width=1))

    # ---- Vertical level lines with staggered top labels ----
    line_levels = [
        {"x": spot_price, "dash": "dashed",  "color": "#ffffff", "text": "SP",  "width": 1.8},
        {"x": mp_strike,  "dash": "dotted",  "color": "#5b8dea", "text": "MP",  "width": 1.5},
        {"x": flip_strike,"dash": [6, 3],     "color": "#5b8dea", "text": "GF",  "width": 1.5},
    ]
    line_levels.extend(
        {"x": float(lvl), "dash": [4, 4], "color": "#13b955", "text": f"P{i}", "width": 1.3}
        for i, lvl in enumerate(pos_gamma, start=1)
    )
    line_levels.extend(
        {"x": float(lvl), "dash": [4, 4], "color": "#ea3943", "text": f"N{i}", "width": 1.3}
        for i, lvl in enumerate(neg_gamma, start=1)
    )
    line_levels.extend(
        {"x": float(lvl), "dash": [8, 4, 2, 4], "color": "#5b8dea", "text": f"A{i}", "width": 1.3}
        for i, lvl in enumerate(a_levels, start=1)
    )

    sorted_levels = sorted(line_levels, key=lambda d: d["x"])
    strike_span = max(
        gex_by_strike["strike"].max() - gex_by_strike["strike"].min(), 1.0
    )
    overlap_threshold = strike_span * 0.015
    # Assign stagger tiers so nearby labels stack upward rather than overlap
    tiers = []
    for item in sorted_levels:
        tier = 0
        for prev_x, prev_tier in reversed(tiers):
            if abs(item["x"] - prev_x) <= overlap_threshold:
                tier = prev_tier + 1
                break
        tiers.append((item["x"], tier))
        item["tier"] = tier

    for item in sorted_levels:
        gex_p.add_layout(Span(
            location=item["x"], dimension="height",
            line_color=item["color"], line_dash=item["dash"],
            line_width=item.get("width", 1.3), line_alpha=0.9,
        ))
        y_px_offset = 6 + item["tier"] * 20
        lbl = Label(
            x=item["x"], y=0, x_units="data", y_units="screen",
            y_offset=y_px_offset,
            text=f"{item['text']} {fmt_price(item['x'])}",
            text_color=item["color"],
            text_font=_BK_MONO, text_font_size="10px",
            text_align="center", text_baseline="bottom",
            background_fill_color="#141720", background_fill_alpha=0.92,
            border_line_color=item["color"], border_line_alpha=0.7,
            border_line_width=1,
        )
        gex_p.add_layout(lbl)

    # ---- Hover: attach to an invisible vline scatter so src_hover columns work ----
    invis = gex_p.scatter("strike", "net_gex", source=src_hover,
                          size=14, alpha=0, line_alpha=0)
    hover = gex_p.select_one(HoverTool)
    if hover:
        hover.renderers = [invis]
        hover.tooltips = [
            ("Strike",   "@strike{0,0}"),
            ("Net GEX",  "@net_gex{0,0} k"),
            ("Call GEX", "@call_gex{0,0} k"),
            ("Put GEX",  "@put_gex{0,0} k"),
        ]
        hover.mode = "vline"

    # ---- Auto-scale left y-axis ----
    left_vals = []
    if "net_gex" in selected_metric_values:
        for exchange_name in exchange_names:
            left_vals.extend((
                dff[dff["exchange"] == exchange_name]
                .groupby("strike", as_index=False)[["total_gex"]]
                .sum()["total_gex"] / NET_GEX_SCALE
            ).tolist())
    if "call_gex" in selected_metric_values:
        left_vals.extend((gex_by_strike["call_gex"] / NET_GEX_SCALE).tolist())
    if "put_gex" in selected_metric_values:
        left_vals.extend((gex_by_strike["put_gex"] / NET_GEX_SCALE).tolist())
    if left_vals:
        max_abs = max(abs(min(left_vals)), abs(max(left_vals)), 1.0)
        gex_p.y_range = Range1d(-max_abs * 1.18, max_abs * 1.18)

    # Format only the primary (left) y-axis; the right AG axis keeps its own formatter
    gex_p.yaxis[0].formatter = NumeralTickFormatter(format="0,0")
    gex_p.xaxis.formatter = NumeralTickFormatter(format="0,0")
    if gex_p.legend:
        gex_p.legend.orientation = "horizontal"
        gex_p.legend.location = "top_left"
        gex_p.legend.click_policy = "hide"
    _bk_theme(gex_p)
    fig = _bk_html(gex_p)

    selected_expiries_dt = pd.to_datetime(selected_expiries)
    symbol_filtered = data[
        (data["symbol"] == symbol)
        & (data["exchange"].isin(selected_exchanges))
        & (data["expiry"].isin(selected_expiries_dt))
    ]
    if symbol_filtered.empty:
        symbol_filtered = data[(data["symbol"] == symbol) & (data["exchange"].isin(selected_exchanges))]

    spot_levels = {"spot": None, "mp": None, "flip": None, "p1": None, "p2": None, "n1": None, "n2": None, "a1": None, "a2": None}
    if not symbol_filtered.empty:
        symbol_calc = symbol_filtered.assign(
            total_gex=symbol_filtered["call_gex"] + symbol_filtered["put_gex"],
            abs_gex=symbol_filtered["call_gex"].abs() + symbol_filtered["put_gex"].abs(),
        )
        symbol_by_strike = symbol_calc.groupby("strike", as_index=False)[["total_gex", "abs_gex"]].sum()
        symbol_pos = symbol_by_strike[symbol_by_strike["total_gex"] > 0].nlargest(2, "total_gex")["strike"].tolist()
        symbol_neg = symbol_by_strike[symbol_by_strike["total_gex"] < 0].nsmallest(2, "total_gex")["strike"].tolist()
        symbol_abs = symbol_by_strike.nlargest(2, "abs_gex")["strike"].tolist()
        spot_levels["spot"] = float(symbol_calc["spot_price"].iloc[-1])
        spot_levels["mp"] = float(symbol_by_strike.loc[symbol_by_strike["total_gex"].abs().idxmin(), "strike"])
        flip_value_symbol = gamma_flip_level(symbol_by_strike)
        spot_levels["flip"] = float(flip_value_symbol) if flip_value_symbol is not None and pd.notna(flip_value_symbol) else spot_levels["mp"]
        if len(symbol_pos) > 0:
            spot_levels["p1"] = float(symbol_pos[0])
        if len(symbol_pos) > 1:
            spot_levels["p2"] = float(symbol_pos[1])
        if len(symbol_neg) > 0:
            spot_levels["n1"] = float(symbol_neg[0])
        if len(symbol_neg) > 1:
            spot_levels["n2"] = float(symbol_neg[1])
        if len(symbol_abs) > 0:
            spot_levels["a1"] = float(symbol_abs[0])
        if len(symbol_abs) > 1:
            spot_levels["a2"] = float(symbol_abs[1])

    deribit_candles = []
    if view_mode == "spot" and spot_source == "deribit":
        deribit_instrument = "BTC-PERPETUAL" if symbol == "BTC" else "ETH-PERPETUAL"
        if use_replay:
            start_ts, end_ts = resolve_replay_window(replay_start, replay_end)
            replay_end_ts = pd.to_datetime(replay_timestamp, utc=True) if replay_timestamp else end_ts
            try:
                deribit_candles = fetch_deribit_candles(
                    instrument_name=deribit_instrument,
                    resolution=str(timeframe),
                    start_ts=start_ts,
                    end_ts=replay_end_ts,
                )
            except Exception:
                deribit_candles = []
        else:
            if session_bars is None:
                lookback_minutes = DERIBIT_LOOKBACK_BY_RESOLUTION.get(str(timeframe), DERIBIT_LOOKBACK_MINUTES)
            else:
                lookback_minutes = max(
                    DERIBIT_LOOKBACK_BY_RESOLUTION.get(str(timeframe), DERIBIT_LOOKBACK_MINUTES),
                    int(session_bars * resolution_to_minutes(timeframe) * 1.3),
                )
            try:
                deribit_candles = fetch_deribit_candles(
                    instrument_name=deribit_instrument,
                    resolution=str(timeframe),
                    lookback_minutes=lookback_minutes,
                )
            except Exception:
                deribit_candles = []

    if spot_source == "deribit" and deribit_candles:
        spot_price = float(deribit_candles[-1]["close"])
        spot_levels["spot"] = spot_price

    spot_fig = build_spot_figure(
        spot_series_points,
        spot_levels,
        deribit_candles=deribit_candles,
        max_bars=session_bars,
        asset_symbol=symbol,
        fill_gaps=fill_spot_gaps,
        smooth_window=smooth_window,
        fixed_range=fixed_range,
    )

    def metric_at_strike(source_df, strike_value, metric_col):
        if source_df.empty or strike_value is None:
            return None
        idx = (source_df["strike"] - float(strike_value)).abs().idxmin()
        return float(source_df.loc[idx, metric_col])

    level_cards = [
        {
            "group": "pivot",
            "name": "SP",
            "badge": "SP",
            "badge_cls": "pill-neutral",
            "tone": "tone-neutral",
            "price": spot_price,
            "metric": None,
        },
        {
            "group": "pivot",
            "name": "MP",
            "badge": "MP",
            "badge_cls": "pill-neutral",
            "tone": "tone-neutral",
            "price": mp_strike,
            "metric": metric_at_strike(gex_by_strike, mp_strike, "total_gex"),
        },
        {
            "group": "pivot",
            "name": "GF",
            "badge": "GF",
            "badge_cls": "pill-neutral",
            "tone": "tone-neutral",
            "price": flip_strike,
            "metric": metric_at_strike(gex_by_strike, flip_strike, "total_gex"),
        },
    ]

    for i, value in enumerate(neg_gamma, start=1):
        level_cards.append(
            {
                "group": "negative",
                "name": f"N{i}",
                "badge": f"N{i}",
                "badge_cls": "pill-neg",
                "tone": "tone-neg",
                "price": float(value),
                "metric": metric_at_strike(gex_by_strike, float(value), "total_gex"),
            }
        )
    for i, value in enumerate(pos_gamma, start=1):
        level_cards.append(
            {
                "group": "positive",
                "name": f"P{i}",
                "badge": f"P{i}",
                "badge_cls": "pill-pos",
                "tone": "tone-pos",
                "price": float(value),
                "metric": metric_at_strike(gex_by_strike, float(value), "total_gex"),
            }
        )
    for i, value in enumerate(a_levels, start=1):
        level_cards.append(
            {
                "group": "absolute",
                "name": f"A{i}",
                "badge": f"A{i}",
                "badge_cls": "pill-abs",
                "tone": "tone-abs",
                "price": float(value),
                "metric": metric_at_strike(gex_by_strike, float(value), "abs_gex"),
            }
        )

    filter_value = level_filter or "all"
    if filter_value == "all":
        visible_cards = level_cards
    else:
        visible_cards = [card for card in level_cards if card["group"] == filter_value]

    level_rows = []
    for card in visible_cards:
        metric_text = fmt_metric(card["metric"])
        level_rows.append(
            html.Div(
                className=f"level-item {card['tone']}",
                children=[
                    html.Div(
                        className="level-item-head",
                        children=[
                            html.Div(card["name"], className="level-name"),
                            html.Span(card["badge"], className=f"level-pill {card['badge_cls']}"),
                        ],
                    ),
                    html.Div(fmt_price(card["price"]), className="level-price"),
                    html.Div(metric_text, className="level-metric"),
                ],
            )
        )

    levels_panel = html.Div(className="levels-list", children=level_rows if level_rows else [html.Div("No levels for this filter", className="panel-subtitle")])
    strategy_panel, ideas_data = build_expiry_strategy_panel(symbol, dff)
    exchange_source = f"{len(selected_exchanges)} exch ({', '.join(selected_exchanges)})"
    if len(selected_expiries) == 1:
        flip_source_note = f"source: 1 expiry ({pd.Timestamp(selected_expiries.min()).strftime('%Y-%m-%d')}) | {exchange_source}"
    else:
        flip_source_note = (
            f"source: {len(selected_expiries)} expiries "
            f"({pd.Timestamp(selected_expiries.min()).strftime('%Y-%m-%d')} to {pd.Timestamp(selected_expiries.max()).strftime('%Y-%m-%d')}) | {exchange_source}"
        )
    metric_caption = ", ".join(GEX_METRIC_LABELS.get(m, m) for m in selected_metric_values)
    gex_caption = f"GEX by Strike | {metric_caption} | {symbol} | {len(selected_expiries)} expiries | {exchange_caption}"
    selected_caption = spot_caption if view_mode == "spot" else gex_caption
    selected_fig = spot_fig if view_mode == "spot" else fig

    return (
        selected_fig,
        levels_panel,
        fmt_price(spot_price),
        fmt_price(mp_strike),
        fmt_price(flip_strike),
        flip_source_note,
        f"{net_total:,.0f}",
        heat_card,
        strategy_panel,
        selected_caption,
        heat_fig,
        heat_summary,
        ideas_data,
    )


@app.callback(
    Output("strategy-suite-builder-store", "data", allow_duplicate=True),
    Output("active-page", "data", allow_duplicate=True),
    Input({"type": "apply-idea-btn", "index": ALL}, "n_clicks"),
    State("idea-apply-store", "data"),
    State("symbol", "value"),
    State("strategy-suite-builder-store", "data"),
    prevent_initial_call=True,
)
def apply_idea_to_suite(n_clicks_list, ideas_data, symbol, current_builder):
    """Convert a trade idea into strategy suite builder legs and jump to the Strategy page."""
    no_nav = dash.no_update
    if not n_clicks_list or not any(n_clicks_list):
        return current_builder or {}, no_nav
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_builder or {}, no_nav
    triggered = ctx.triggered[0]
    prop_id = triggered["prop_id"]
    if triggered["value"] is None:
        return current_builder or {}, no_nav
    try:
        idx = json.loads(prop_id.rsplit(".", 1)[0])["index"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return current_builder or {}, no_nav
    if not ideas_data or idx >= len(ideas_data):
        return current_builder or {}, no_nav
    idea = ideas_data[idx]
    ticket = idea.get("ticket") or {}
    ticket_legs = ticket.get("legs") or []
    idea_expiry = str(idea.get("expiry") or "")
    builder_legs = []
    for i, leg in enumerate(ticket_legs[:4]):
        side = str(leg.get("side", "buy")).lower()
        action = "sell" if side in ("sell", "short") else "buy"
        option_type = str(leg.get("type", "call")).lower()
        if option_type not in ("call", "put"):
            option_type = "call"
        strike = leg.get("strike")
        try:
            strike = float(strike) if strike is not None else None
        except (TypeError, ValueError):
            strike = None
        builder_legs.append({
            "row_id": i + 1,
            "enabled": True,
            "action": action,
            "type": option_type,
            "expiry": idea_expiry,
            "strike": strike,
            "quantity": 1.0,
        })
    while len(builder_legs) < 4:
        builder_legs.append({
            "row_id": len(builder_legs) + 1,
            "enabled": False,
            "action": "buy",
            "type": "call",
            "expiry": idea_expiry,
            "strike": None,
            "quantity": 0.0,
        })
    return (
        {
            "symbol": str(symbol or "BTC").upper(),
            "template": "custom",
            "commission": float((current_builder or {}).get("commission", 0.5)),
            "eval_days": float((current_builder or {}).get("eval_days", 7)),
            "name": str(idea.get("name", "Applied Idea")),
            "legs": builder_legs,
        },
        "strategy",
    )


@app.callback(
    Output("replay-timestamp", "options"),
    Output("replay-timestamp", "value"),
    Output("replay-status", "children"),
    Output("replay-play-state", "data"),
    Output("replay-play", "disabled"),
    Output("replay-play-btn", "children"),
    Output("replay-play-btn", "disabled"),
    Output("replay-play-btn-chart", "children"),
    Output("replay-play-btn-chart", "disabled"),
    Output("replay-step-btn", "disabled"),
    Input("symbol", "value"),
    Input("replay-range", "start_date"),
    Input("replay-range", "end_date"),
    Input("health-refresh", "n_intervals"),
    Input("replay-play-btn", "n_clicks"),
    Input("replay-play-btn-chart", "n_clicks"),
    Input("replay-step-btn", "n_clicks"),
    Input("replay-play", "n_intervals"),
    Input("data-mode", "value"),
    State("replay-timestamp", "value"),
    State("replay-timestamp", "options"),
    State("replay-play-state", "data"),
)
def update_replay_controls(
    symbol,
    start_date,
    end_date,
    _n_intervals,
    _play_clicks,
    _play_clicks_chart,
    _step_clicks,
    _play_ticks,
    data_mode,
    current_value,
    current_options,
    play_state,
):
    ctx = dash.callback_context
    trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else ""
    playing = bool((play_state or {}).get("playing"))

    options = current_options or []
    should_refresh_options = trigger in {"symbol", "replay-range", "health-refresh"} or not options
    if should_refresh_options:
        start_ts, end_ts = resolve_replay_window(start_date, end_date)
        timestamps = load_snapshot_timestamps(SNAPSHOT_DB, symbol, start_ts=start_ts, end_ts=end_ts, limit=200)
        options = [{"label": pd.to_datetime(ts, utc=True).strftime("%Y-%m-%d %H:%M UTC"), "value": ts} for ts in timestamps]

    values = [item["value"] for item in options]
    values_sorted = sorted(
        values,
        key=lambda v: pd.to_datetime(v, utc=True) if v else pd.Timestamp.min.tz_localize("UTC"),
    )
    status = f"Snapshots: {len(values)}"

    controls_disabled = data_mode != "replay" or not values_sorted
    if data_mode != "replay":
        return (
            options,
            current_value,
            status,
            {"playing": False},
            True,
            "Play",
            True,
            "Play",
            True,
            True,
        )

    if trigger in {"symbol", "replay-range"}:
        playing = False

    if trigger in {"replay-play-btn", "replay-play-btn-chart"}:
        if not values_sorted:
            return (
                options,
                current_value,
                status,
                {"playing": False},
                True,
                "Play",
                True,
                "Play",
                True,
                True,
            )
        playing = not playing
        if playing and current_value not in values:
            current_value = values_sorted[0]
        label = "Pause" if playing else "Play"
        return options, current_value, status, {"playing": playing}, not playing, label, controls_disabled, label, controls_disabled, controls_disabled

    if trigger == "replay-step-btn":
        if values_sorted:
            try:
                idx = values_sorted.index(current_value)
            except ValueError:
                idx = -1
            next_idx = min(idx + 1, len(values_sorted) - 1)
            current_value = values_sorted[next_idx]
        playing = False
        return options, current_value, status, {"playing": False}, True, "Play", controls_disabled, "Play", controls_disabled, controls_disabled

    if trigger == "replay-play":
        if not playing:
            return options, current_value, status, {"playing": False}, True, "Play", controls_disabled, "Play", controls_disabled, controls_disabled
        if not values_sorted:
            return options, current_value, status, {"playing": False}, True, "Play", controls_disabled, "Play", controls_disabled, controls_disabled
        try:
            idx = values_sorted.index(current_value)
        except ValueError:
            idx = -1
        next_idx = idx + 1
        if next_idx >= len(values_sorted):
            return options, values_sorted[-1], status, {"playing": False}, True, "Play", controls_disabled, "Play", controls_disabled, controls_disabled
        return options, values_sorted[next_idx], status, {"playing": True}, False, "Pause", controls_disabled, "Pause", controls_disabled, controls_disabled

    if current_value not in values:
        current_value = values[0] if values else None

    label = "Pause" if playing else "Play"
    return options, current_value, status, {"playing": playing}, not playing, label, controls_disabled, label, controls_disabled, controls_disabled


@app.callback(
    Output("replay-scale-store", "data"),
    Input("symbol", "value"),
    Input("replay-range", "start_date"),
    Input("replay-range", "end_date"),
    Input("spot-source", "value"),
    Input("data-mode", "value"),
)
def compute_replay_scale(symbol, start_date, end_date, spot_source, data_mode):
    if data_mode != "replay" or not symbol:
        return {}
    spot_source = spot_source or "spot"
    start_ts, end_ts = resolve_replay_window(start_date, end_date)
    min_p = None
    max_p = None
    if spot_source == "spot":
        df = load_snapshot_range(SNAPSHOT_DB, symbol, start_ts, end_ts)
        if df is None or df.empty or "spot_price" not in df.columns:
            return {}
        series = df.groupby("ts_utc")["spot_price"].median()
        if series.empty:
            return {}
        min_p = float(series.min())
        max_p = float(series.max())
    else:
        deribit_instrument = "BTC-PERPETUAL" if symbol == "BTC" else "ETH-PERPETUAL"
        try:
            candles = fetch_deribit_candles(
                instrument_name=deribit_instrument,
                resolution=str(DERIBIT_RESOLUTION),
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except Exception:
            return {}
        if not candles:
            return {}
        lows = [float(c["low"]) for c in candles if c.get("low") is not None]
        highs = [float(c["high"]) for c in candles if c.get("high") is not None]
        if not lows or not highs:
            return {}
        min_p = min(lows)
        max_p = max(highs)
    if min_p is None or max_p is None or max_p <= min_p:
        return {}
    span = max(max_p - min_p, max(abs(max_p), 1.0) * 0.02)
    pad = span * 0.08
    return {"min": float(min_p - pad), "max": float(max_p + pad)}


@app.callback(
    Output("replay-timestamp", "disabled"),
    Input("data-mode", "value"),
)
def toggle_replay_timestamp_disabled(data_mode):
    return data_mode != "replay"


@app.callback(
    Output("dealer-flow-panel", "children"),
    Output("vanna-charm-chart", "srcDoc"),
    Output("vanna-charm-summary", "children"),
    Input("symbol", "value"),
    Input("exchange-selector", "value"),
    Input("expiry-selector", "value"),
    Input("chart-refresh", "n_intervals"),
    Input("data-mode", "value"),
    Input("replay-timestamp", "value"),
)
def update_higher_greeks_panels(symbol, selected_exchanges, selected_expiries,
                                _chart_intervals, data_mode, replay_timestamp):
    use_replay = data_mode == "replay" and bool(replay_timestamp)
    data = get_latest_data()

    # Build filtered df for Dealer Flow (always uses aggregated GEX from CSV/snapshot)
    if use_replay:
        snapshot = load_snapshot_at(SNAPSHOT_DB, symbol, replay_timestamp)
        if snapshot is None or snapshot.empty:
            dff = pd.DataFrame()
            spot = None
        else:
            snap = snapshot.copy()
            snap["exchange"] = "Snapshot"
            snap["call_oi"] = 0.0
            snap["put_oi"] = 0.0
            dff = snap
            spot = float(dff["spot_price"].iloc[-1]) if not dff.empty else None
    else:
        if data is None or data.empty:
            dff = pd.DataFrame()
            spot = None
        else:
            sel_ex = normalize_exchange_selection(selected_exchanges, data)
            dff = data[(data["symbol"] == symbol) & (data["exchange"].isin(sel_ex))]
            if selected_expiries:
                exp_dt = pd.to_datetime(selected_expiries)
                dff = dff[dff["expiry"].isin(exp_dt)]
            spot = float(dff["spot_price"].iloc[-1]) if not dff.empty else None

    flow_children = build_dealer_flow_panel_children(dff, spot)

    # Vanna / charm always uses the live Deribit option chain (only Deribit ships per-strike IV in the chain summary).
    chain_df = pd.DataFrame()
    if not use_replay:
        try:
            sym_key = str(symbol or "BTC").upper()
            cached_df = CHAIN_CACHE.get(sym_key)
            if cached_df is not None and not cached_df.empty:
                chain_df = cached_df
            if chain_df.empty:
                chain_df = fetch_deribit_option_chain(sym_key)
                if chain_df is not None and not chain_df.empty:
                    CHAIN_CACHE.set(sym_key, chain_df)
        except Exception:
            chain_df = pd.DataFrame()

    if chain_df is None or chain_df.empty:
        vc_html, vc_summary = build_empty_figure(
            "Vanna / charm requires a live Deribit chain (with mark IV per strike)."
        ), "Live chain unavailable."
    else:
        # Filter chain by selected expiries if any
        chain_used = chain_df
        if selected_expiries:
            try:
                wanted = {pd.to_datetime(e).strftime("%Y-%m-%d") for e in selected_expiries}
                chain_used = chain_df[chain_df["expiry"].isin(wanted)]
                if chain_used.empty:
                    chain_used = chain_df
            except Exception:
                chain_used = chain_df
        chain_spot = spot
        if chain_spot is None:
            try:
                chain_spot = get_chain_spot(chain_used, str(symbol or "BTC").upper())
            except Exception:
                chain_spot = None
        vc_html, vc_summary = build_vanna_charm_chart(chain_used, chain_spot)

    return flow_children, vc_html, vc_summary


def _get_cached_chain(symbol: str) -> pd.DataFrame:
    """Shared chain fetcher backed by the TTLCache."""
    sym = str(symbol or "BTC").upper()
    cached_df = CHAIN_CACHE.get(sym)
    if cached_df is not None and not cached_df.empty:
        return cached_df
    try:
        df = fetch_deribit_option_chain(sym)
        if df is not None and not df.empty:
            CHAIN_CACHE.set(sym, df)
            return df
    except Exception:
        pass
    return pd.DataFrame()


def _atm_iv_from_chain(chain_df: pd.DataFrame, spot):
    if chain_df is None or chain_df.empty or not spot:
        return None
    df = chain_df[pd.notna(chain_df.get("iv"))]
    if df.empty:
        return None
    # Pick the front expiry, then the strike closest to spot, then average call/put IV
    df = df.copy()
    df["expiry_dt"] = pd.to_datetime(df["expiry"], errors="coerce")
    df = df.dropna(subset=["expiry_dt"])
    if df.empty:
        return None
    front_expiry = df["expiry_dt"].min()
    front = df[df["expiry_dt"] == front_expiry]
    if front.empty:
        return None
    nearest_strike = front.iloc[(front["strike"] - float(spot)).abs().argsort()[:2]]
    iv_vals = nearest_strike["iv"].astype(float)
    iv_vals = iv_vals[iv_vals > 0]
    if iv_vals.empty:
        return None
    return float(iv_vals.mean())


@app.callback(
    Output("workspace-store", "data"),
    Output("workspace-status", "children"),
    Input("workspace-save-btn", "n_clicks"),
    State("symbol", "value"),
    State("exchange-selector", "value"),
    State("expiry-selector", "value"),
    State("data-mode", "value"),
    State("active-page", "data"),
    prevent_initial_call=True,
)
def workspace_save(n_clicks, symbol, exchanges, expiries, data_mode, active_page):
    if not n_clicks:
        return dash.no_update, dash.no_update
    payload = {
        "symbol": symbol,
        "exchanges": exchanges,
        "expiries": expiries,
        "data_mode": data_mode,
        "active_page": active_page,
        "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    return payload, "Workspace saved"


@app.callback(
    Output("symbol", "value", allow_duplicate=True),
    Output("exchange-selector", "value", allow_duplicate=True),
    Output("expiry-selector", "value", allow_duplicate=True),
    Output("data-mode", "value", allow_duplicate=True),
    Output("active-page", "data", allow_duplicate=True),
    Output("workspace-status", "children", allow_duplicate=True),
    Input("workspace-load-btn", "n_clicks"),
    State("workspace-store", "data"),
    prevent_initial_call=True,
)
def workspace_load(n_clicks, store_data):
    if not n_clicks or not store_data:
        return (
            dash.no_update, dash.no_update, dash.no_update,
            dash.no_update, dash.no_update,
            "No saved workspace" if n_clicks else dash.no_update,
        )
    return (
        store_data.get("symbol") or dash.no_update,
        store_data.get("exchanges") or dash.no_update,
        store_data.get("expiries") or dash.no_update,
        store_data.get("data_mode") or dash.no_update,
        store_data.get("active_page") or dash.no_update,
        f"Loaded ({store_data.get('saved_at', '')[:16]})",
    )


@app.callback(
    Output("watchlist-row", "children"),
    Input("symbol", "value"),
    Input("chart-refresh", "n_intervals"),
    Input("spot-refresh", "n_intervals"),
)
def update_watchlist(active_symbol, _chart_n, _spot_n):
    data = get_latest_data()
    if data is None or data.empty:
        return [html.Div("Watchlist: no data loaded", className="watchlist-tile-empty")]
    symbols = sorted([s for s in data["symbol"].dropna().unique().tolist() if s])
    tiles = []
    for sym in symbols:
        sym_df = data[data["symbol"] == sym]
        if sym_df.empty:
            continue
        try:
            spot = float(sym_df["spot_price"].iloc[-1])
        except Exception:
            spot = None
        sym_by_strike = (
            sym_df.groupby("strike", as_index=False)
            .agg(call_gex=("call_gex", "sum"), put_gex=("put_gex", "sum"))
        )
        sym_by_strike["total_gex"] = sym_by_strike["call_gex"] + sym_by_strike["put_gex"]
        net = float(sym_by_strike["total_gex"].sum()) if not sym_by_strike.empty else 0.0
        flip = gamma_flip_level(sym_by_strike)
        regime = "MR" if net >= 0 else "TF"
        regime_class = "regime-mr" if regime == "MR" else "regime-tf"
        active_class = " active" if sym == active_symbol else ""
        spot_str = fmt_price(spot) if spot is not None else "—"
        flip_str = fmt_price(flip) if flip is not None and pd.notna(flip) else "—"
        net_str = f"NetGEX {fmt_metric(net)}$ · Flip {flip_str}"
        tiles.append(
            html.Div(
                id={"type": "watchlist-tile", "symbol": sym},
                className=f"watchlist-tile {regime_class}{active_class}",
                n_clicks=0,
                children=[
                    html.Div(sym, className="watchlist-tile-sym"),
                    html.Div(spot_str, className="watchlist-tile-spot"),
                    html.Div(net_str, className="watchlist-tile-net"),
                    html.Div(regime, className="watchlist-tile-regime"),
                ],
            )
        )
    if not tiles:
        return [html.Div("Watchlist: no symbols found", className="watchlist-tile-empty")]
    return tiles


@app.callback(
    Output("symbol", "value", allow_duplicate=True),
    Input({"type": "watchlist-tile", "symbol": ALL}, "n_clicks"),
    State("symbol", "value"),
    prevent_initial_call=True,
)
def watchlist_select_symbol(_clicks, current_symbol):
    ctx = dash.callback_context
    if not ctx.triggered or not any(_clicks or []):
        return dash.no_update
    try:
        triggered = ctx.triggered[0]["prop_id"].split(".")[0]
        meta = json.loads(triggered)
        new_sym = meta.get("symbol")
        if new_sym and new_sym != current_symbol:
            return new_sym
    except Exception:
        pass
    return dash.no_update


def _quant_panel_error(message: str):
    """Render an error placeholder for a single quant panel."""
    return _bk_html(_bk_theme(build_empty_figure(message))), f"Panel error: {message}"


@app.callback(
    Output("intraday-gex-chart", "srcDoc"),
    Output("intraday-gex-summary", "children"),
    Input("symbol", "value"),
    Input("chart-refresh", "n_intervals"),
)
def update_intraday_gex_panel(symbol, _n):
    sym = str(symbol or "BTC").upper()
    try:
        try:
            metric_df = load_metric_history(SNAPSHOT_DB, sym, limit=400)
        except Exception:
            metric_df = pd.DataFrame()
        snap_df = pd.DataFrame()
        try:
            end_ts = pd.Timestamp.now(tz="UTC")
            start_ts = end_ts - pd.Timedelta(days=2)
            snap_df = load_snapshot_range(SNAPSHOT_DB, sym, start_ts, end_ts)
        except Exception:
            snap_df = pd.DataFrame()
        fig = build_intraday_gex_figure(metric_df, snap_df)
        if metric_df is None or metric_df.empty:
            summary = "No GEX history yet \u2014 collector writes snapshots to gex_snapshots.db."
        else:
            last_net = float(metric_df["net_gex"].iloc[-1])
            regime = "Mean-Reverting" if last_net >= 0 else "Trend-Following"
            summary = f"{len(metric_df)} samples \u00b7 last NetGEX {fmt_metric(last_net)}$ \u00b7 {regime}"
        return _bk_html(_bk_theme(fig)), summary
    except Exception as exc:
        return _quant_panel_error(f"intraday GEX failed: {exc}")


@app.callback(
    Output("vol-surface-chart", "srcDoc"),
    Output("vol-surface-summary", "children"),
    Input("symbol", "value"),
    Input("chart-refresh", "n_intervals"),
)
def update_vol_surface_panel(symbol, _n):
    sym = str(symbol or "BTC").upper()
    try:
        chain_df = _get_cached_chain(sym)
        if chain_df is None or chain_df.empty:
            fig = build_vol_surface_figure(pd.DataFrame(), None)
            summary = "Live chain unavailable."
        else:
            try:
                chain_spot = get_chain_spot(chain_df, sym)
            except Exception:
                chain_spot = None
            fig = build_vol_surface_figure(chain_df, chain_spot)
            n_strikes = chain_df["strike"].nunique() if "strike" in chain_df.columns else 0
            n_exp = chain_df["expiry"].nunique() if "expiry" in chain_df.columns else 0
            summary = f"{n_strikes} strikes \u00d7 {n_exp} expiries from Deribit mark IV"
        return _bk_html(_bk_theme(fig)), summary
    except Exception as exc:
        return _quant_panel_error(f"vol surface failed: {exc}")


@app.callback(
    Output("rv-iv-chart", "srcDoc"),
    Output("rv-iv-summary", "children"),
    Input("symbol", "value"),
    Input("chart-refresh", "n_intervals"),
)
def update_rv_iv_panel(symbol, _n):
    sym = str(symbol or "BTC").upper()
    try:
        try:
            instrument = f"{sym}-PERPETUAL"
            candles = fetch_deribit_candles(instrument_name=instrument, resolution="60", lookback_minutes=14 * 24 * 60)
        except Exception:
            candles = []
        candles_df = pd.DataFrame(candles) if candles else pd.DataFrame()
        if candles_df.empty:
            return _bk_html(_bk_theme(build_rv_iv_figure(pd.DataFrame(), None))), "Spot candles unavailable."
        try:
            chain_spot_for_iv = float(candles_df["close"].iloc[-1])
        except Exception:
            chain_spot_for_iv = None
        chain_df = _get_cached_chain(sym)
        atm_iv = _atm_iv_from_chain(chain_df, chain_spot_for_iv) if not chain_df.empty else None
        fig = build_rv_iv_figure(candles_df, atm_iv, window=30)
        rv_series = parkinson_vol(candles_df, window=30)
        vrp = vol_risk_premium(rv_series, atm_iv)
        if vrp is None:
            summary = "ATM IV unavailable \u2014 VRP cannot be computed."
        else:
            tone = "rich" if vrp > 0 else "cheap"
            summary = f"VRP {vrp:+.1f}% \u00b7 premium {tone} \u00b7 Parkinson(30) on {len(candles_df)} bars"
        return _bk_html(_bk_theme(fig)), summary
    except Exception as exc:
        return _quant_panel_error(f"RV/IV failed: {exc}")


@app.callback(
    Output("hedge-backtest-chart", "srcDoc"),
    Output("hedge-backtest-summary", "children"),
    Input("symbol", "value"),
    Input("chart-refresh", "n_intervals"),
)
def update_hedge_backtest_panel(symbol, _n):
    sym = str(symbol or "BTC").upper()
    try:
        try:
            hist_end = pd.Timestamp.now(tz="UTC")
            hist_start = hist_end - pd.Timedelta(days=14)
            hist_df = load_snapshot_range(SNAPSHOT_DB, sym, hist_start, hist_end)
        except Exception:
            hist_df = pd.DataFrame()
        bt_result = build_hedge_backtest_figure(hist_df)
        if isinstance(bt_result, tuple):
            fig, meta = bt_result
        else:
            fig, meta = bt_result, None
        if meta:
            summary = (
                f"{meta['samples']} samples \u00b7 hit rate {meta['hit_rate'] * 100:.0f}% "
                f"\u00b7 current call: {meta['last_pred']}"
            )
        else:
            summary = "Need a few more snapshots before backtest is meaningful."
        return _bk_html(_bk_theme(fig)), summary
    except Exception as exc:
        return _quant_panel_error(f"hedge backtest failed: {exc}")


@app.callback(
    Output("alert-state", "data"),
    Output("alerts-last-refresh", "children"),
    Input("alerts-refresh", "n_intervals"),
    Input("symbol", "value"),
    Input("exchange-selector", "value"),
    Input("expiry-selector", "value"),
    State("alert-state", "data"),
)
def evaluate_alerts_callback(_n_intervals, symbol, selected_exchanges, selected_expiries, alert_state):
    if not symbol:
        return alert_state or {}, "Alerts: idle"
    data = get_latest_data()
    if data is None or data.empty:
        return alert_state or {}, "Alerts: no data"
    selected_exchanges = normalize_exchange_selection(selected_exchanges, data)
    dff = data[(data["symbol"] == symbol) & (data["exchange"].isin(selected_exchanges))]
    if selected_expiries:
        selected_expiries = pd.to_datetime(selected_expiries)
        dff = dff[dff["expiry"].isin(selected_expiries)]
    if dff.empty:
        return alert_state or {}, "Alerts: no rows"
    work = dff.assign(total_gex=dff["call_gex"] + dff["put_gex"], abs_gex=dff["call_gex"].abs() + dff["put_gex"].abs())
    gex_by_strike = work.groupby("strike", as_index=False)[["total_gex", "abs_gex"]].sum()
    spot = float(work["spot_price"].iloc[-1])
    flip = gamma_flip_level(gex_by_strike)
    net_gex = float(gex_by_strike["total_gex"].sum())
    oi_wall = get_oi_wall(work)
    expiries = sorted(work["expiry"].dt.strftime("%Y-%m-%d").unique().tolist())
    vol_regime = get_cached_vol_regime(symbol, expiries, spot)

    prev_flip = (alert_state or {}).get("prev_flip")
    prev_oi_wall = (alert_state or {}).get("prev_oi_wall")
    prev_vol_regime = (alert_state or {}).get("prev_vol_regime")
    context = {
        "symbol": symbol,
        "spot": spot,
        "prev_flip": prev_flip,
        "curr_flip": flip,
        "net_gex": net_gex,
        "oi_wall": oi_wall,
        "prev_oi_wall": prev_oi_wall,
        "vol_regime": vol_regime,
        "prev_vol_regime": prev_vol_regime,
        "ts_utc": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    alerts = evaluate_alert_rules(context, rules_path=ALERT_RULES_FILE)
    channels = [c.strip().lower() for c in str(ALERT_CHANNELS or "").split(",") if c.strip()]
    now = pd.Timestamp.now(tz="UTC")
    new_count = 0
    for alert in alerts:
        rule_id = alert.get("rule_id", "unknown")
        last_ts = get_last_alert_ts(SNAPSHOT_DB, symbol, rule_id)
        if last_ts is not None and ALERT_THROTTLE_MIN > 0:
            if (now - last_ts).total_seconds() < (ALERT_THROTTLE_MIN * 60):
                continue
        write_alert(
            SNAPSHOT_DB,
            symbol=symbol,
            rule_id=rule_id,
            severity=alert.get("severity", "medium"),
            message=alert.get("message", "alert"),
            payload=alert.get("payload", {}),
            ts_utc=now,
        )
        new_count += 1
        if "webhook" in channels and ALERT_WEBHOOK_URL:
            send_webhook_alert(ALERT_WEBHOOK_URL, alert)
    next_state = {"prev_flip": flip, "prev_oi_wall": oi_wall, "prev_vol_regime": vol_regime}
    refresh_note = f"Alert scan {now.strftime('%H:%M:%S UTC')} | new {new_count}"
    return next_state, refresh_note


@app.callback(
    Output("alerts-panel", "children"),
    Output("alerts-status", "children"),
    Output("alerts-ack-status", "children"),
    Input("alert-state", "data"),
    Input("alert-severity-filter", "value"),
    Input("alert-unacked-only", "value"),
    Input("ack-alerts-btn", "n_clicks"),
    Input("symbol", "value"),
)
def render_alerts_panel(_alert_state, severity_filter, unacked_only, _ack_clicks, symbol):
    ctx = dash.callback_context
    ack_status = ""
    if ctx.triggered:
        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
        if triggered_id == "ack-alerts-btn":
            acked = ack_alerts(SNAPSHOT_DB, symbol)
            ack_status = f"Acknowledged {acked} alerts."
    severities = severity_filter or []
    only_unacked = "unacked" in (unacked_only or [])
    df = load_alerts(SNAPSHOT_DB, symbol=symbol, limit=50, unacked_only=only_unacked, severity_filter=severities)
    if df is None or df.empty:
        return html.Div("No alerts yet", className="panel-subtitle"), "Alerts: 0", ack_status
    rows = []
    for _, row in df.iterrows():
        sev = str(row.get("severity", "medium")).lower()
        badge_cls = f"alert-badge alert-sev-{sev}"
        ts = row.get("ts_utc")
        ts_text = ts.strftime("%Y-%m-%d %H:%M") if pd.notna(ts) else "n/a"
        rows.append(
            html.Div(
                className="alert-row",
                children=[
                    html.Div(
                        className="alert-head",
                        children=[
                            html.Span(ts_text),
                            html.Span(sev, className=badge_cls),
                        ],
                    ),
                    html.Div(str(row.get("message", "")), className="alert-msg"),
                ],
            )
        )
    status = f"Alerts shown: {len(rows)}"
    return rows, status, ack_status


@app.callback(
    Output("tickets-status", "children"),
    Input("export-tickets-btn", "n_clicks"),
    State("symbol", "value"),
    State("exchange-selector", "value"),
    State("expiry-selector", "value"),
    prevent_initial_call=True,
)
def export_tickets(n_clicks, symbol, selected_exchanges, selected_expiries):
    if not n_clicks:
        return ""
    data = get_latest_data()
    selected_exchanges = normalize_exchange_selection(selected_exchanges, data)
    dff = data[(data["symbol"] == symbol) & (data["exchange"].isin(selected_exchanges))]
    if selected_expiries:
        selected_expiries = pd.to_datetime(selected_expiries)
        dff = dff[dff["expiry"].isin(selected_expiries)]
    if dff.empty:
        return "No data to generate tickets."
    stability = get_stability_status(symbol)
    stability_cv = parse_stability_cv(stability.get("detail"))
    pro_payload = generate_professional_ideas(dff, symbol=symbol, account_equity=ACCOUNT_EQUITY_USD, stability_cv=stability_cv)
    ideas = pro_payload.get("ideas") or []
    tickets = []
    for idea in ideas:
        ticket = idea.get("ticket")
        if ticket:
            tickets.append({"idea": idea.get("name"), "ticket": ticket})
    if not tickets:
        return "No tickets available from current ideas."
    out_dir = Path(DATA_DIR) / "tickets"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"tickets_{symbol}_{stamp}.json"
    out_path.write_text(json.dumps({"symbol": symbol, "tickets": tickets}, indent=2), encoding="utf-8")
    return f"Exported {len(tickets)} ticket(s) to {out_path.name}"


@app.callback(
    Output("positions-table", "children"),
    Input("positions-store", "data"),
)
def render_positions_table(data):
    records = list(data or [])
    if not records:
        return html.Div("No positions loaded.", className="panel-subtitle")
    rows = [
        html.Div(
            className="table-row table-header",
            children=[
                html.Div("Symbol"),
                html.Div("Expiry"),
                html.Div("Strike"),
                html.Div("Type"),
                html.Div("Qty"),
                html.Div("Avg Px"),
                html.Div(""),
            ],
        )
    ]
    for idx, row in enumerate(records):
        rows.append(
            html.Div(
                className="table-row",
                children=[
                    html.Div(str(row.get("symbol", "")).upper(), className="table-cell"),
                    html.Div(str(row.get("expiry", "")), className="table-cell"),
                    html.Div(str(row.get("strike", "")), className="table-cell"),
                    html.Div(str(row.get("type", "")).lower(), className="table-cell"),
                    html.Div(str(row.get("quantity", "")), className="table-cell"),
                    html.Div(str(row.get("avg_price", "")), className="table-cell"),
                    html.Button(
                        "Remove",
                        id={"type": "pos-remove", "index": idx},
                        className="action-button secondary small",
                    ),
                ],
            )
        )
    return rows


@app.callback(
    Output("positions-store", "data"),
    Output("positions-status", "children"),
    Input("add-position-btn", "n_clicks"),
    Input("save-positions-btn", "n_clicks"),
    Input("load-positions-btn", "n_clicks"),
    Input("clear-positions-btn", "n_clicks"),
    Input({"type": "pos-remove", "index": ALL}, "n_clicks"),
    State("position-symbol", "value"),
    State("position-expiry", "date"),
    State("position-strike", "value"),
    State("position-type", "value"),
    State("position-quantity", "value"),
    State("position-avg-price", "value"),
    State("positions-store", "data"),
)
def edit_positions(
    add_clicks,
    save_clicks,
    load_clicks,
    clear_clicks,
    remove_clicks,
    symbol,
    expiry,
    strike,
    option_type,
    quantity,
    avg_price,
    current_data,
):
    ctx = dash.callback_context
    data = list(current_data or [])
    if not ctx.triggered:
        return data, ""
    trigger = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger == "load-positions-btn":
        df = load_positions(POSITIONS_FILE)
        return serialize_positions(df), f"Loaded {len(df)} positions from CSV."
    if trigger == "clear-positions-btn":
        return [], "Cleared positions (unsaved)."
    if trigger == "save-positions-btn":
        df = normalize_positions(pd.DataFrame(data))
        POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        df = df[POSITION_COLUMNS]
        df.to_csv(POSITIONS_FILE, index=False)
        return data, f"Saved {len(df)} positions to {POSITIONS_FILE.name}."
    if trigger == "add-position-btn":
        missing = []
        if not symbol:
            missing.append("symbol")
        if not expiry:
            missing.append("expiry")
        if strike in (None, ""):
            missing.append("strike")
        if not option_type:
            missing.append("type")
        if quantity in (None, ""):
            missing.append("quantity")
        if missing:
            return data, f"Missing: {', '.join(missing)}"
        expiry_ts = pd.to_datetime(expiry, errors="coerce")
        if pd.isna(expiry_ts):
            return data, "Expiry must be a date."
        try:
            strike_val = float(strike)
            qty_val = float(quantity)
            avg_val = float(avg_price) if avg_price not in (None, "") else 0.0
        except (TypeError, ValueError):
            return data, "Strike, quantity, and avg price must be numbers."
        if strike_val <= 0:
            return data, "Strike must be greater than 0."
        if qty_val == 0:
            return data, "Quantity cannot be 0."
        record = {
            "symbol": str(symbol).upper().strip(),
            "expiry": expiry_ts.date().isoformat(),
            "strike": strike_val,
            "type": str(option_type).lower().strip(),
            "quantity": qty_val,
            "avg_price": avg_val,
        }
        data.append(record)
        return data, f"Added {record['symbol']} {record['type']} {record['strike']:.0f} x {record['quantity']:+.2f}."
    if trigger.startswith("{"):
        try:
            payload = json.loads(trigger)
            if payload.get("type") == "pos-remove":
                idx = int(payload.get("index"))
                if 0 <= idx < len(data):
                    removed = data.pop(idx)
                    label = f"{removed.get('symbol', '')} {removed.get('type', '')} {removed.get('strike', '')}"
                    return data, f"Removed {label.strip()}."
        except (ValueError, TypeError, json.JSONDecodeError):
            return data, "Unable to remove position."
    return data, ""


@app.callback(
    Output("portfolio-summary", "children"),
    Output("portfolio-table", "children"),
    Output("scenario-summary", "children"),
    Input("symbol", "value"),
    Input("spot-shift", "value"),
    Input("vol-shift", "value"),
    Input("health-refresh", "n_intervals"),
    Input("positions-store", "data"),
)
def update_portfolio_panel(symbol, spot_shift, vol_shift, _n_intervals, positions_data):
    positions = normalize_positions(pd.DataFrame(positions_data or []))
    data = get_latest_data()
    report = build_portfolio_snapshot(positions, data, symbol, spot_shift_pct=spot_shift or 0.0, vol_shift_pct=vol_shift or 0.0)
    if not report.get("ok"):
        note = report.get("reason", "portfolio unavailable")
        return html.Div(note, className="panel-subtitle"), html.Div(), ""
    summary = report["summary"]
    cards = [
        html.Div([html.Div("Spot", className="k"), html.Div(fmt_price(summary["spot"]), className="v")], className="info-card"),
        html.Div([html.Div("Net Delta", className="k"), html.Div(f"{summary['net_delta']:+.2f}", className="v")], className="info-card"),
        html.Div([html.Div("Net Gamma", className="k"), html.Div(f"{summary['net_gamma']:+.4f}", className="v")], className="info-card"),
        html.Div([html.Div("Net Vega", className="k"), html.Div(f"{summary['net_vega']:+.2f}", className="v")], className="info-card"),
        html.Div([html.Div("Net GEX", className="k"), html.Div(fmt_metric(summary["net_gex"]), className="v")], className="info-card"),
    ]
    table_rows = [
        html.Div(
            className="table-row table-header",
            children=[
                html.Div("Expiry"),
                html.Div("Delta"),
                html.Div("Gamma"),
                html.Div("Vega"),
                html.Div("GEX"),
                html.Div("P&L"),
            ],
        )
    ]
    by_expiry = report["by_expiry"].head(6)
    for _, row in by_expiry.iterrows():
        table_rows.append(
            html.Div(
                className="table-row",
                children=[
                    html.Div(str(row.get("expiry")), className="table-cell"),
                    html.Div(f"{row.get('net_delta', 0):+.2f}", className="table-cell"),
                    html.Div(f"{row.get('net_gamma', 0):+.4f}", className="table-cell"),
                    html.Div(f"{row.get('net_vega', 0):+.2f}", className="table-cell"),
                    html.Div(fmt_metric(row.get("net_gex", 0)), className="table-cell"),
                    html.Div(f"{row.get('pnl', 0):+.0f}", className="table-cell"),
                ],
            )
        )
    scenario_text = (
        f"Scenario spot {summary['scenario_spot']:.0f} (shift {spot_shift or 0:.1f}%) | "
        f"vol {summary['scenario_vol']:.2f} (shift {vol_shift or 0:.0f}%) | "
        f"P&L {summary['pnl_scn']:+.0f}"
    )
    return html.Div(children=cards, className="portfolio-cards"), html.Div(children=table_rows, className="mini-table"), scenario_text


@app.callback(
    Output("data-health-panel", "children"),
    Input("health-refresh", "n_intervals"),
    Input("symbol", "value"),
)
def update_data_health(_n_intervals, symbol):
    data = get_latest_data()
    try:
        stale_minutes = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(OPTIONS_FILE.stat().st_mtime, unit="s", tz="UTC")).total_seconds() / 60.0
    except Exception:
        stale_minutes = None
    df_symbol = data[data["symbol"] == symbol] if data is not None and not data.empty else pd.DataFrame()
    report = health_report(df_symbol, stale_minutes, MIN_STRATEGY_ROWS, MIN_STRATEGY_STRIKES)
    latest_metric = load_latest_metric(SNAPSHOT_DB, symbol) or {}
    collector_status = read_collector_status()

    rows = [
        html.Div(
            className="telegram-row",
            children=[
                html.Div("Freshness", className="telegram-label"),
                html.Span("n/a" if stale_minutes is None else f"{stale_minutes:.1f}m", className="telegram-value"),
            ],
        ),
        html.Div(
            className="telegram-row",
            children=[
                html.Div("Rows/Strikes", className="telegram-label"),
                html.Span(f"{len(df_symbol)} / {int(df_symbol['strike'].nunique()) if not df_symbol.empty else 0}", className="telegram-value"),
            ],
        ),
        html.Div(
            className="telegram-row",
            children=[
                html.Div("Health", className="telegram-label"),
                html.Span("OK" if report.get("ok") else ", ".join(report.get("issues", []) or ["issues"]), className="telegram-value"),
            ],
        ),
    ]
    if latest_metric:
        rows.append(
            html.Div(
                className="telegram-row",
                children=[
                    html.Div("Last Metric", className="telegram-label"),
                    html.Span(f"{latest_metric['ts_utc'].strftime('%H:%M UTC')} | {latest_metric['net_gex']:+.0f}", className="telegram-value"),
                ],
            )
        )
    if collector_status:
        rows.append(
            html.Div(
                className="telegram-row",
                children=[
                    html.Div("Collector", className="telegram-label"),
                    html.Span("OK" if collector_status.get("ok") else collector_status.get("error", "error"), className="telegram-value"),
                ],
            )
        )
    return html.Div(className="telegram-grid", children=rows)


@app.callback(
    Output("export-snapshot-status", "children"),
    Input("export-snapshot-btn", "n_clicks"),
    State("symbol", "value"),
    State("replay-range", "start_date"),
    State("replay-range", "end_date"),
    prevent_initial_call=True,
)
def export_snapshot_csv(n_clicks, symbol, start_date, end_date):
    if not n_clicks:
        return ""
    now = pd.Timestamp.now(tz="UTC")
    start_ts = pd.to_datetime(start_date).tz_localize("UTC") if start_date else now - pd.Timedelta(days=REPLAY_DEFAULT_DAYS)
    end_ts = pd.to_datetime(end_date).tz_localize("UTC") + pd.Timedelta(days=1) if end_date else now
    df = load_snapshot_range(SNAPSHOT_DB, symbol, start_ts, end_ts)
    if df is None or df.empty:
        return "No snapshots found for export."
    out_dir = Path(DATA_DIR) / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"snapshots_{symbol}_{stamp}.csv"
    df.to_csv(out_path, index=False)
    return f"Exported {len(df)} rows to {out_path.name}"


# ── Options Selling Backtest callback ──────────────────────────────────

def _fmt_money(v, decimals=0):
    """Format a number as money with sign."""
    if v >= 0:
        return f"+${v:,.{decimals}f}" if v > 0 else f"${v:,.{decimals}f}"
    return f"-${abs(v):,.{decimals}f}"


def _build_stat_card(label, value, color="#e0e0e0"):
    return html.Div(
        style={
            "background": "#1a1a2e", "borderRadius": "8px", "padding": "12px 16px",
            "textAlign": "center", "border": f"1px solid {color}33",
        },
        children=[
            html.Div(label, style={"fontSize": "11px", "color": "#888", "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "18px", "fontWeight": "bold", "color": color}),
        ],
    )


@app.callback(
    Output("bt-stats-cards", "children"),
    Output("bt-equity-chart", "figure"),
    Output("bt-pnl-chart", "figure"),
    Output("bt-trade-log", "children"),
    Output("bt-next-trade", "children"),
    Output("bt-next-trade-store", "data"),
    Output("bt-status", "children"),
    Input("bt-run-btn", "n_clicks"),
    State("bt-symbol", "value"),
    State("bt-strategy", "value"),
    State("bt-cycle", "value"),
    State("bt-delta", "value"),
    State("bt-days", "value"),
    State("bt-capital", "value"),
    State("bt-reinvest", "value"),
    prevent_initial_call=True,
)
def run_backtest_callback(n_clicks, symbol, strategy, cycle, delta, days, capital, reinvest):
    if not n_clicks:
        return [], go.Figure(), go.Figure(), [], [], {}, ""

    symbol = str(symbol or "BTC").upper()
    strategy = str(strategy or "short_put")
    cycle = str(cycle or "weekly")
    delta = float(delta or 0.15)
    days = int(days or 365)
    capital = float(capital or 100000)

    reinvest_on = bool(reinvest and "yes" in reinvest)

    try:
        result = run_options_backtest(
            symbol=symbol,
            strategy=strategy,
            cycle=cycle,
            target_delta=delta,
            capital=capital,
            days=days,
            reinvest=reinvest_on,
        )
    except Exception as exc:
        return [], go.Figure(), go.Figure(), [], [], {}, f"Error: {exc}"

    if result.total_trades == 0:
        return [], go.Figure(), go.Figure(), [], [], {}, "No trades generated. Try a longer lookback or different settings."

    strategy_labels = {
        "short_put": "Short Put", "cash_secured_put": "Cash-Secured Put",
        "short_call": "Short Call",
        "short_strangle": "Short Strangle", "iron_condor": "Iron Condor",
        "covered_call": "Covered Call", "covered_put": "Covered Put",
    }
    strat_label = strategy_labels.get(strategy, strategy)

    # Stats cards
    pnl_color = "#4ade80" if result.total_pnl >= 0 else "#f87171"
    cards = html.Div(
        style={"display": "grid", "gridTemplateColumns": "repeat(8, 1fr)", "gap": "10px"},
        children=[
            _build_stat_card("Total Trades", str(result.total_trades)),
            _build_stat_card("Win Rate", f"{result.win_rate:.1%}", "#4ade80" if result.win_rate >= 0.5 else "#f87171"),
            _build_stat_card("Total PnL", _fmt_money(result.total_pnl), pnl_color),
            _build_stat_card("Avg PnL / Trade", _fmt_money(result.avg_pnl), "#4ade80" if result.avg_pnl >= 0 else "#f87171"),
            _build_stat_card("Avg Premium", f"${result.avg_premium:,.0f}", "#60a5fa"),
            _build_stat_card("Max Win", _fmt_money(result.max_win), "#4ade80"),
            _build_stat_card("Max Loss", _fmt_money(result.max_loss), "#f87171"),
            _build_stat_card("Sharpe", f"{result.sharpe:.2f}", "#c084fc"),
        ],
    )

    # Equity curve
    eq_fig = go.Figure()
    eq_dates = result.dates + ([result.trades[-1]["expiry_date"]] if result.trades else [])
    eq_fig.add_trace(go.Scatter(
        x=eq_dates,
        y=result.equity_curve,
        mode="lines",
        fill="tozeroy",
        line=dict(color="#60a5fa", width=2),
        fillcolor="rgba(96,165,250,0.1)",
        name="Equity",
    ))
    eq_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0d0d1a",
        plot_bgcolor="#0d0d1a",
        title=dict(text=f"{symbol} {strat_label} ({cycle}, {delta:.0%}\u0394{'', ' \u2022 Reinvested'}[reinvest_on]) \u2014 Equity Curve", font=dict(size=14)),
        xaxis=dict(title="Date", gridcolor="#222"),
        yaxis=dict(title="Equity ($)", gridcolor="#222", tickformat="$,.0f"),
        margin=dict(l=60, r=20, t=40, b=40),
        height=350,
    )
    # Draw the starting capital line
    eq_fig.add_hline(y=capital, line_dash="dash", line_color="#555", annotation_text=f"Start ${capital:,.0f}")
    # Max drawdown annotation
    if result.max_drawdown > 0:
        eq_fig.add_annotation(
            x=eq_dates[-1], y=min(result.equity_curve),
            text=f"Max DD: {result.max_drawdown:.2%}",
            showarrow=False, font=dict(color="#f87171", size=11),
            yshift=-15,
        )

    # PnL per trade bar chart
    pnl_fig = go.Figure()
    trade_pnls = [t["pnl"] for t in result.trades]
    trade_dates = [t["entry_date"] for t in result.trades]
    colors = ["#4ade80" if p >= 0 else "#f87171" for p in trade_pnls]
    pnl_fig.add_trace(go.Bar(
        x=trade_dates,
        y=trade_pnls,
        marker_color=colors,
        name="PnL",
    ))
    pnl_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0d0d1a",
        plot_bgcolor="#0d0d1a",
        title=dict(text="PnL Per Trade", font=dict(size=14)),
        xaxis=dict(title="Entry Date", gridcolor="#222"),
        yaxis=dict(title="PnL ($)", gridcolor="#222", tickformat="$,.0f"),
        margin=dict(l=60, r=20, t=40, b=40),
        height=300,
    )
    pnl_fig.add_hline(y=0, line_color="#555")

    # Trade log table
    header_style = {"background": "#1a1a2e", "color": "#888", "padding": "8px 10px", "fontSize": "11px", "textAlign": "left", "borderBottom": "1px solid #333"}
    cell_style = {"padding": "6px 10px", "fontSize": "12px", "borderBottom": "1px solid #1a1a2e"}

    header_cols = ["Entry", "Expiry", "Spot", "Settlement"]
    if strategy in ("short_put", "cash_secured_put", "short_strangle", "iron_condor", "covered_put"):
        header_cols.append("Put K")
    if strategy in ("short_call", "short_strangle", "iron_condor", "covered_call"):
        header_cols.append("Call K")
    if strategy == "iron_condor":
        header_cols += ["Long Put K", "Long Call K"]
    header_cols += ["Size", "Contracts", "Premium", "PnL", "W/L"]

    table_header = html.Tr([html.Th(c, style=header_style) for c in header_cols])
    table_rows = []
    for t in reversed(result.trades):  # newest first
        pnl_c = "#4ade80" if t["pnl"] >= 0 else "#f87171"
        cells = [
            html.Td(t["entry_date"], style=cell_style),
            html.Td(t["expiry_date"], style=cell_style),
            html.Td(f"${t['spot']:,.0f}", style=cell_style),
            html.Td(f"${t['settlement']:,.0f}", style=cell_style),
        ]
        if strategy in ("short_put", "cash_secured_put", "short_strangle", "iron_condor", "covered_put"):
            cells.append(html.Td(f"${t.get('put_strike', 0):,.0f}", style=cell_style))
        if strategy in ("short_call", "short_strangle", "iron_condor", "covered_call"):
            cells.append(html.Td(f"${t.get('call_strike', 0):,.0f}", style=cell_style))
        if strategy == "iron_condor":
            cells.append(html.Td(f"${t.get('long_put_strike', 0):,.0f}", style=cell_style))
            cells.append(html.Td(f"${t.get('long_call_strike', 0):,.0f}", style=cell_style))
        cells += [
            html.Td(f"${t.get('position_size', 0):,.0f}", style=cell_style),
            html.Td(f"{t.get('contracts', 0):,.4f}", style=cell_style),
            html.Td(f"${t['premium']:,.0f}", style=cell_style),
            html.Td(f"${t['pnl']:,.0f}", style={**cell_style, "color": pnl_c, "fontWeight": "bold"}),
            html.Td("W" if t["won"] else "L", style={**cell_style, "color": pnl_c}),
        ]
        table_rows.append(html.Tr(cells))

    trade_table = html.Table(
        [html.Thead(table_header), html.Tbody(table_rows)],
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "12px"},
    )

    # Build "Next Trade Idea" card using live spot + RV, reinvested equity if enabled.
    try:
        next_equity = result.equity_curve[-1] if (reinvest_on and result.equity_curve) else None
        suggestion = suggest_next_trade(
            symbol=symbol,
            strategy=strategy,
            cycle=cycle,
            target_delta=delta,
            capital=capital,
            reinvest_equity=next_equity,
        )
    except Exception as exc:
        suggestion = {"ok": False, "reason": f"{exc}"}
    next_card = _build_next_trade_card(suggestion, strat_label)

    reinvest_tag = " | Reinvested" if reinvest_on else ""
    status = f"{strat_label} on {symbol} ({cycle}, {delta:.0%}\u0394{reinvest_tag}) \u2014 {result.total_trades} trades over {days}d"
    store_payload = suggestion if isinstance(suggestion, dict) and suggestion.get("ok") else {}
    return cards, eq_fig, pnl_fig, trade_table, next_card, store_payload, status


def _build_next_trade_card(s: dict, strat_label: str):
    card_bg = "#0f1628"
    border = "1px solid #2a3150"
    label_style = {"color": "#7d8597", "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "0.5px"}
    value_style = {"color": "#e5e7eb", "fontSize": "14px", "fontWeight": "600", "marginTop": "2px"}
    pair_style = {"display": "flex", "flexDirection": "column", "padding": "8px 12px"}

    if not s or not s.get("ok"):
        return html.Div(
            [
                html.Div("Next Trade Idea", style={"fontSize": "13px", "fontWeight": "bold", "color": "#c084fc", "marginBottom": "6px"}),
                html.Div(s.get("reason", "Unavailable") if isinstance(s, dict) else "Unavailable",
                         style={"color": "#f87171", "fontSize": "12px"}),
            ],
            style={"padding": "12px 14px", "background": card_bg, "border": border, "borderRadius": "6px"},
        )

    expiry_suffix = "" if s.get("listed_expiry", False) else " (theoretical)"
    pairs = [
        ("Entry", s["entry_date"]),
        ("Expiry", f"{s['expiry_date']} ({s['days_to_expiry']}d){expiry_suffix}"),
        ("Spot", f"${s['spot']:,.0f}"),
        ("IV used", f"{s['iv'] * 100:.1f}%"),
    ]
    if s.get("put_strike") is not None:
        pairs.append(("Put K", f"${s['put_strike']:,.0f} (Δ {s.get('put_delta', 0):.2f})"))
    if s.get("put_instrument"):
        pairs.append(("Put inst.", s["put_instrument"]))
    if s.get("call_strike") is not None:
        pairs.append(("Call K", f"${s['call_strike']:,.0f} (Δ {s.get('call_delta', 0):.2f})"))
    if s.get("call_instrument"):
        pairs.append(("Call inst.", s["call_instrument"]))
    if s.get("long_put_strike") is not None:
        pairs.append(("Long Put K", f"${s['long_put_strike']:,.0f}"))
    if s.get("long_put_instrument"):
        pairs.append(("Long put inst.", s["long_put_instrument"]))
    if s.get("long_call_strike") is not None:
        pairs.append(("Long Call K", f"${s['long_call_strike']:,.0f}"))
    if s.get("long_call_instrument"):
        pairs.append(("Long call inst.", s["long_call_instrument"]))

    pairs += [
        ("Contracts", f"{s['contracts']:.4f}"),
        ("Size deployed", f"${s['position_size']:,.0f}"),
        ("Premium est.", f"${s['expected_premium']:,.0f}"),
        ("Max loss", "uncapped" if s.get("max_loss") is None else f"${s['max_loss']:,.0f}"),
    ]

    if not s.get("tradeable", True):
        pairs.append(("Warning", s.get("reason", "not tradeable")))

    grid = html.Div(
        [
            html.Div([
                html.Div(label, style=label_style),
                html.Div(value, style=value_style),
            ], style=pair_style)
            for label, value in pairs
        ],
        style={"display": "grid", "gridTemplateColumns": "repeat(auto-fill, minmax(160px, 1fr))", "gap": "4px"},
    )

    send_btn = html.Button(
        "Send to Suite",
        id="bt-send-to-suite-btn",
        n_clicks=0,
        className="action-button",
        disabled=not s.get("tradeable", True),
        style={"marginTop": "10px"},
    )
    send_status = html.Div(
        id="bt-send-to-suite-status",
        style={"fontSize": "11px", "color": "#7d8597", "marginTop": "6px"},
    )

    return html.Div(
        [
            html.Div(
                f"Next Trade Idea \u2014 {strat_label}",
                style={"fontSize": "13px", "fontWeight": "bold", "color": "#c084fc", "marginBottom": "6px"},
            ),
            html.Div(
                "Based on live Deribit spot + realised-vol premium bump; sized by exchange increments.",
                style={"fontSize": "11px", "color": "#7d8597", "marginBottom": "8px"},
            ),
            grid,
            send_btn,
            send_status,
        ],
        style={"padding": "12px 14px", "background": card_bg, "border": border, "borderRadius": "6px"},
    )


def _suggestion_to_legs(s: dict) -> list:
    strategy = str(s.get("strategy") or "").lower()
    expiry = s.get("expiry_date")
    qty = float(s.get("contracts") or 0.0)
    legs: list = []

    def _leg(row_id: int, action: str, leg_type: str, strike):
        return {
            "row_id": row_id,
            "enabled": True,
            "action": action,
            "type": leg_type,
            "expiry": expiry,
            "strike": float(strike) if strike is not None else None,
            "quantity": qty,
        }

    if strategy in ("short_put", "cash_secured_put"):
        legs.append(_leg(1, "sell", "put", s.get("put_strike")))
    elif strategy == "short_call":
        legs.append(_leg(1, "sell", "call", s.get("call_strike")))
    elif strategy == "short_strangle":
        legs.append(_leg(1, "sell", "put", s.get("put_strike")))
        legs.append(_leg(2, "sell", "call", s.get("call_strike")))
    elif strategy == "iron_condor":
        legs.append(_leg(1, "sell", "put", s.get("put_strike")))
        legs.append(_leg(2, "buy", "put", s.get("long_put_strike")))
        legs.append(_leg(3, "sell", "call", s.get("call_strike")))
        legs.append(_leg(4, "buy", "call", s.get("long_call_strike")))
    return legs


_SUGGESTION_TEMPLATE_MAP = {
    "short_put": "short_put",
    "cash_secured_put": "short_put",
    "short_call": "short_call",
    "short_strangle": "short_strangle",
    "iron_condor": "iron_condor",
}


@app.callback(
    Output("bt-send-to-suite-status", "children"),
    Input("bt-send-to-suite-btn", "n_clicks"),
    State("bt-next-trade-store", "data"),
    prevent_initial_call=True,
)
def send_idea_to_suite(n_clicks, suggestion):
    if not n_clicks:
        return ""
    if not isinstance(suggestion, dict) or not suggestion.get("ok"):
        return "No trade idea available to send."
    if not suggestion.get("tradeable", True):
        return "Idea is not tradeable — nothing sent."

    legs = _suggestion_to_legs(suggestion)
    if not legs:
        return f"Unsupported strategy: {suggestion.get('strategy')}"

    symbol = str(suggestion.get("symbol") or "BTC").upper()
    strategy = str(suggestion.get("strategy") or "short_put").lower()
    template = _SUGGESTION_TEMPLATE_MAP.get(strategy, "custom")
    entry = suggestion.get("entry_date") or pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
    expiry = suggestion.get("expiry_date") or ""
    name = f"BT {symbol} {strategy} {entry}->{expiry}".strip()

    record = {
        "symbol": symbol,
        "template": template,
        "commission": float(DEFAULT_COMMISSION_PER_CONTRACT),
        "eval_days": float(suggestion.get("days_to_expiry") or 7),
        "name": name,
        "legs": legs,
    }
    try:
        save_strategy(STRATEGY_SUITE_SAVE_PATH, record)
    except Exception as exc:
        return f"Save failed: {exc}"
    return f"Sent to Suite as '{name}'."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
