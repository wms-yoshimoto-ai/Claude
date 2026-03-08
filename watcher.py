#!/usr/bin/env python3
"""
watcher.py - Mac 上で常駐し fetch_trigger.json を監視する
~/Library/Scripts/GoogleAds/watcher.py に配置して使用
"""

import json
import time
import subprocess
import sys
from pathlib import Path
from datetime import datetime

# ============================================================
# パス設定
# ============================================================
HOME          = Path.home()
FETCHER_DIR   = HOME / "Desktop" / "Claude" / "GoogleAds_Fetcher"
TRIGGER_FILE  = FETCHER_DIR / "fetch_trigger.json"
STATUS_FILE   = FETCHER_DIR / "fetch_status.json"
RUNNER        = HOME / "Library" / "Scripts" / "GoogleAds" / "run_fetcher.sh"
LOG_FILE      = HOME / "Library" / "Scripts" / "GoogleAds" / "watcher.log"


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    with open(LOG_FILE, "a") as f:
        f.write(line)
    print(line, end="", flush=True)


def main():
    log(f"Python watcher 起動 (PID={os.getpid() if 'os' in dir() else 'N/A'})")
    log(f"監視ファイル: {TRIGGER_FILE}")
    log(f"実行スクリプト: {RUNNER}")

    # 起動チェック
    if not FETCHER_DIR.exists():
        log(f"エラー: FETCHER_DIR が見つかりません: {FETCHER_DIR}")
        sys.exit(1)
    if not RUNNER.exists():
        log(f"エラー: run_fetcher.sh が見つかりません: {RUNNER}")
        sys.exit(1)

    last_requested_at = ""

    while True:
        try:
            if TRIGGER_FILE.exists():
                content = TRIGGER_FILE.read_text(encoding="utf-8")
                d = json.loads(content)
                current = d.get("requested_at", "")

                if current and current != last_requested_at:
                    log(f"新しいトリガー検知: action={d.get('action')} site={d.get('site')} requested_at={current}")
                    last_requested_at = current

                    # run_fetcher.sh を非同期実行
                    proc = subprocess.Popen(
                        ["/bin/bash", str(RUNNER)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    log(f"run_fetcher.sh 起動 (PID={proc.pid})")

        except json.JSONDecodeError as e:
            log(f"JSON解析エラー（書き込み中の可能性）: {e}")
        except Exception as e:
            log(f"エラー: {type(e).__name__}: {e}")

        time.sleep(5)


if __name__ == "__main__":
    import os
    main()
