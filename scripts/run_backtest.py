from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import SNAPSHOT_DB, SUPPORTED_SYMBOLS
from pro.backtest import run_walk_forward_backtest


def main():
    for symbol in SUPPORTED_SYMBOLS:
        result = run_walk_forward_backtest(SNAPSHOT_DB, symbol)
        print(symbol, result)


if __name__ == "__main__":
    main()
