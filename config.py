"""Shared project configuration."""

from pathlib import Path
import os

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""

BASE_DIR = Path(__file__).resolve().parent
OPTIONS_FILE = BASE_DIR / "options_data.csv"
DATA_DIR = BASE_DIR / "data"
SNAPSHOT_DB = DATA_DIR / "gex_snapshots.db"

# Data collection defaults
SUPPORTED_SYMBOLS = ("BTC", "ETH")
SUPPORTED_EXCHANGES = ("Deribit", "Bybit", "Binance", "OKX")
GEX_NOISE_THRESHOLD = 1e7
ORDERBOOK_WORKERS = 15

# Strategy/risk defaults
ACCOUNT_EQUITY_USD = float(os.getenv("ACCOUNT_EQUITY_USD", "100000"))
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# Alerting + ticketing
ALERT_RULES_FILE = Path(os.getenv("ALERT_RULES_FILE", str(DATA_DIR / "alert_rules.json")))
ALERT_THROTTLE_MIN = _env_int("ALERT_THROTTLE_MIN", 15)
ALERT_CHANNELS = os.getenv("ALERT_CHANNELS", "webhook")

# Portfolio
POSITIONS_FILE = Path(os.getenv("POSITIONS_FILE", str(DATA_DIR / "positions.csv")))
POSITION_SOURCE = os.getenv("POSITION_SOURCE", "csv")

# Telegram integration (bot + channel)
TELEGRAM_BOT_TOKEN_FILE = os.getenv("TELEGRAM_BOT_TOKEN_FILE", str(DATA_DIR / "telegram_token.txt"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = _read_text_file(Path(TELEGRAM_BOT_TOKEN_FILE))
TELEGRAM_BOT_HANDLE = os.getenv("TELEGRAM_BOT_HANDLE", "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
TELEGRAM_CHANNEL_URL = os.getenv("TELEGRAM_CHANNEL_URL", "")
TELEGRAM_ALLOWED_CHAT_IDS = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "")
TELEGRAM_DEFAULT_SYMBOL = os.getenv("TELEGRAM_DEFAULT_SYMBOL", "BTC")
TELEGRAM_DEFAULT_DTE_DAYS = _env_int("TELEGRAM_DEFAULT_DTE_DAYS", 7)
TELEGRAM_DEFAULT_EXCHANGES = os.getenv("TELEGRAM_DEFAULT_EXCHANGES", "")
