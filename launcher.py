"""One-command launcher for the GEX dashboard stack.

Starts:
1) dependency install (optional)
2) initial collection
3) periodic collector loop in background
4) Dash app server in foreground
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import threading
import time
import traceback
import webbrowser
from pathlib import Path

from collector import run_collection


ROOT = Path(__file__).resolve().parent


def _run_pip_install() -> None:
    requirements = ROOT / "requirements.txt"
    if not requirements.exists():
        print("[launcher] requirements.txt not found, skipping install")
        return
    print("[launcher] Installing dependencies from requirements.txt ...")
    subprocess.run(
        ["python", "-m", "pip", "install", "-r", str(requirements)],
        cwd=str(ROOT),
        check=True,
    )


def _run_initial_collection(retries: int, retry_delay_sec: int) -> None:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[launcher] Initial collection attempt {attempt}/{retries} ...")
            df = run_collection()
            print(f"[launcher] Initial collection complete: {len(df)} rows")
            return
        except Exception as exc:  # pragma: no cover - runtime guard
            last_err = exc
            print(f"[launcher] Initial collection failed: {exc}")
            if attempt < retries:
                time.sleep(retry_delay_sec)
    raise RuntimeError(f"Initial collection failed after {retries} attempts: {last_err}")


def _collector_loop(interval_sec: int, stop_event: threading.Event) -> None:
    print(f"[launcher] Collector loop started (interval={interval_sec}s)")
    while not stop_event.is_set():
        start = time.time()
        try:
            df = run_collection()
            print(f"[launcher] Collector update complete: {len(df)} rows")
        except Exception as exc:  # pragma: no cover - runtime guard
            print(f"[launcher] Collector loop error: {exc}")
            traceback.print_exc()

        elapsed = max(0.0, time.time() - start)
        sleep_left = max(1.0, interval_sec - elapsed)
        stop_event.wait(timeout=sleep_left)
    print("[launcher] Collector loop stopped")


def _start_telegram_bot(skip: bool) -> subprocess.Popen | None:
    if skip:
        print("[launcher] Telegram bot skipped (flag)")
        return None
    try:
        from config import TELEGRAM_BOT_TOKEN
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"[launcher] Telegram bot config error: {exc}")
        return None
    if not TELEGRAM_BOT_TOKEN:
        print("[launcher] Telegram bot not configured; skipping")
        return None
    bot_path = ROOT / "scripts" / "telegram_levels_bot.py"
    if not bot_path.exists():
        print("[launcher] Telegram bot script missing; skipping")
        return None
    print("[launcher] Starting Telegram bot ...")
    return subprocess.Popen(["python", str(bot_path)], cwd=str(ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch GEX dashboard + auto collector")
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Dash host (default: 0.0.0.0 — reachable from LAN; use 127.0.0.1 for local-only)",
    )
    parser.add_argument("--port", type=int, default=8050, help="Dash port (default: 8050)")
    parser.add_argument(
        "--collector-interval",
        type=int,
        default=600,
        help="Collector refresh interval in seconds (default: 600)",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip install -r requirements.txt",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open dashboard URL in browser after start",
    )
    parser.add_argument(
        "--skip-telegram-bot",
        action="store_true",
        help="Skip Telegram bot even if configured",
    )
    parser.add_argument(
        "--initial-retries",
        type=int,
        default=3,
        help="Initial collector retry count (default: 3)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.skip_install:
        _run_pip_install()

    _run_initial_collection(retries=max(1, args.initial_retries), retry_delay_sec=5)

    stop_event = threading.Event()
    collector_thread = threading.Thread(
        target=_collector_loop,
        args=(max(10, int(args.collector_interval)), stop_event),
        daemon=True,
        name="collector-loop",
    )
    collector_thread.start()
    bot_process = _start_telegram_bot(skip=args.skip_telegram_bot)

    # Import after collection so app starts with fresh data loaded.
    from app import app

    url = f"http://{args.host}:{args.port}"
    local_url = f"http://127.0.0.1:{args.port}"
    lan_url: str | None = None
    if args.host in ("0.0.0.0", ""):
        try:
            lan_ip = socket.gethostbyname(socket.gethostname())
            lan_url = f"http://{lan_ip}:{args.port}"
        except Exception:  # pragma: no cover - best effort
            lan_url = None

    if args.open_browser:
        try:
            webbrowser.open(lan_url or local_url)
        except Exception:  # pragma: no cover - best effort
            pass

    print(f"[launcher] Dashboard running at {url}")
    print(f"[launcher]   local:   {local_url}")
    if lan_url:
        print(f"[launcher]   network: {lan_url}")
        print("[launcher] Note: allow Python through Windows Firewall on port "
              f"{args.port} for other devices to connect")
    print("[launcher] Press Ctrl+C to stop")
    try:
        app.run(host=args.host, port=int(args.port), debug=False)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        collector_thread.join(timeout=3)
        if bot_process and bot_process.poll() is None:
            bot_process.terminate()
            try:
                bot_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bot_process.kill()
        print("[launcher] Shutdown complete")


if __name__ == "__main__":
    main()
