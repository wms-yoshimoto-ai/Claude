#!/usr/bin/env python3
"""
trigger_fetch.py
================
Cowork から Mac の fetch スクリプトをトリガーするためのヘルパー。

【使い方】
  from trigger_fetch import request_fetch, wait_for_result, read_result

  # キャンペーンデータ取得
  ok = request_fetch(site="065", date_from="2026-01-01", date_to="2026-02-28")

  # ユーザーの所在地レポート取得
  ok = request_fetch(site="065", date_from="2026-01-01", date_to="2026-02-28",
                     action="fetch_location", campaign="23335569301")
"""

import json
import time
from datetime import datetime
from pathlib import Path

# ============================================================
# パス設定（自動検出 — セッションIDに依存しない）
# ============================================================
def _auto_detect_dir(folder_name: str, mac_path: str) -> Path:
    """マウント済みフォルダを自動検出する。見つからなければ Mac パスを返す。"""
    import glob
    # 1) このファイル自身の場所から推定（GoogleAds_Fetcher 内にいる場合）
    this_dir = Path(__file__).resolve().parent
    if this_dir.name == folder_name:
        return this_dir
    # 2) 同じマウントルートの隣にあるか
    sibling = this_dir.parent / folder_name
    if sibling.exists():
        return sibling
    # 3) /sessions/*/mnt/ 配下を走査
    for pattern in [f"/sessions/*/mnt/*/{folder_name}", f"/sessions/*/mnt/{folder_name}"]:
        hits = glob.glob(pattern)
        for h in hits:
            p = Path(h)
            if p.exists() and p.is_dir():
                return p
    # 4) フォールバック: Mac の実パス
    return Path(mac_path)

FETCHER_DIR  = _auto_detect_dir("GoogleAds_Fetcher", "/Users/yoshimototoshihiro/Desktop/Claude/GoogleAds_Fetcher")
DATA_DIR     = _auto_detect_dir("GoogleAds_Data", "/Users/yoshimototoshihiro/Documents/GoogleAds_Data")
TRIGGER_FILE = FETCHER_DIR / "fetch_trigger.json"
STATUS_FILE  = FETCHER_DIR / "fetch_status.json"


def request_fetch(site: str, date_from: str, date_to: str,
                  action: str = "fetch", campaign: str = "") -> bool:
    """
    fetch_trigger.json に指示を書き込み、Mac 側のスクリプトを起動する。

    Parameters
    ----------
    site      : サイトID（例: "065"）またはアカウント名
    date_from : 開始日 YYYY-MM-DD
    date_to   : 終了日 YYYY-MM-DD
    action    : "fetch"（キャンペーンデータ）or "fetch_location"（所在地レポート）
    campaign  : キャンペーンID（fetch_location の場合に指定）

    Returns
    -------
    bool : トリガーファイルの書き込みに成功したか
    """
    trigger = {
        "action":      action,
        "site":        site,
        "from":        date_from,
        "to":          date_to,
        "campaign":    campaign,
        "requested_at": datetime.now().isoformat(),
    }

    # まずステータスを pending にリセット
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump({"status": "pending", "requested_at": trigger["requested_at"]},
                      f, ensure_ascii=False)
    except Exception:
        pass

    # トリガーを書き込む（launchd がこれを検知して run_fetcher.sh を実行）
    try:
        with open(TRIGGER_FILE, "w", encoding="utf-8") as f:
            json.dump(trigger, f, ensure_ascii=False, indent=2)
        print(f"✓ トリガー送信: {action} | site={site} | {date_from}〜{date_to}")
        return True
    except Exception as e:
        print(f"✗ トリガー送信失敗: {e}")
        return False


def wait_for_result(timeout_sec: int = 300, poll_interval: int = 3) -> dict:
    """
    fetch_status.json を監視して、Mac 側の実行完了を待つ。

    Parameters
    ----------
    timeout_sec   : タイムアウト秒数（デフォルト5分）
    poll_interval : ポーリング間隔（秒）

    Returns
    -------
    dict : ステータス情報
           {"status": "done", "output_file": "...", ...}
           {"status": "error", "message": "...", ...}
           {"status": "timeout", ...}
    """
    print(f"⏳ Mac 側の実行完了を待機中 (最大 {timeout_sec} 秒)...", flush=True)
    deadline = time.time() + timeout_sec
    last_status = ""

    while time.time() < deadline:
        try:
            with open(STATUS_FILE, encoding="utf-8") as f:
                st = json.load(f)
            status = st.get("status", "")

            if status != last_status:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] status={status}", flush=True)
                last_status = status

            if status == "done":
                print(f"✓ 完了: {st.get('output_file', '')}")
                return st
            elif status == "error":
                print(f"✗ エラー: {st.get('message', '不明なエラー')}")
                return st

        except (FileNotFoundError, json.JSONDecodeError):
            pass  # ファイルがまだない or 書き込み中

        time.sleep(poll_interval)

    result = {"status": "timeout", "message": f"{timeout_sec}秒以内に完了しませんでした"}
    print(f"✗ タイムアウト")
    return result


def read_result(status: dict) -> dict | None:
    """
    wait_for_result の戻り値を受け取り、JSON データを読み込む。

    Returns
    -------
    dict or None : データ本体（JSON の内容）
    """
    if status.get("status") != "done":
        print(f"✗ 完了ステータスではありません: {status.get('status')}")
        return None

    output_file = status.get("output_file", "")
    if not output_file:
        print("✗ output_file が指定されていません")
        return None

    # Cowork の VM から見たパスに変換
    # Mac: ~/Documents/GoogleAds_Data/xxx.json
    # VM:  /sessions/.../mnt/GoogleAds_Data/xxx.json
    vm_path = Path(output_file.replace(
        "/Users/yoshimototoshihiro/Documents/GoogleAds_Data",
        str(DATA_DIR)
    ))

    try:
        with open(vm_path, encoding="utf-8") as f:
            data = json.load(f)
        print(f"✓ データ読み込み完了: {vm_path.name}")
        return data
    except Exception as e:
        print(f"✗ ファイル読み込みエラー: {e}")
        return None


def git_push(commit_message: str = "Coworkからの自動コミット", timeout_sec: int = 60) -> dict:
    """
    Coworkで修正したコードをGitHubにプッシュする。

    Example
    -------
    git_push("fetch_google_ads.py: 新しいメトリクスを追加")
    """
    if not request_fetch("", "", "", action="git_push"):
        return {"status": "error", "message": "トリガー送信失敗"}
    # commit_message をトリガーファイルに追記
    try:
        with open(TRIGGER_FILE, encoding="utf-8") as f:
            trigger = json.load(f)
        trigger["message"] = commit_message
        with open(TRIGGER_FILE, "w", encoding="utf-8") as f:
            json.dump(trigger, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠ メッセージ書き込みエラー: {e}")
    return wait_for_result(timeout_sec=timeout_sec)


def git_pull(timeout_sec: int = 60) -> dict:
    """
    GitHubの最新コードをMacに取得する。

    Example
    -------
    git_pull()
    """
    if not request_fetch("", "", "", action="git_pull"):
        return {"status": "error", "message": "トリガー送信失敗"}
    return wait_for_result(timeout_sec=timeout_sec)


def fetch_and_read(site: str, date_from: str, date_to: str,
                   action: str = "fetch", campaign: str = "",
                   timeout_sec: int = 300) -> dict | None:
    """
    トリガー送信 → 待機 → データ読み込みを一括実行するショートカット関数。

    Example
    -------
    data = fetch_and_read("065", "2026-01-01", "2026-02-28")
    print(data["summary"])
    """
    if not request_fetch(site, date_from, date_to, action, campaign):
        return None
    status = wait_for_result(timeout_sec=timeout_sec)
    return read_result(status)


# ============================================================
# 単体テスト用（直接実行した場合）
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--site",     required=True)
    parser.add_argument("--from",     dest="date_from", required=True)
    parser.add_argument("--to",       dest="date_to",   required=True)
    parser.add_argument("--action",   default="fetch")
    parser.add_argument("--campaign", default="")
    args = parser.parse_args()

    data = fetch_and_read(
        site=args.site,
        date_from=args.date_from,
        date_to=args.date_to,
        action=args.action,
        campaign=args.campaign,
    )
    if data:
        print(json.dumps(data, ensure_ascii=False, indent=2)[:500] + "...")
