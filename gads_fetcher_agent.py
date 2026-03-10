#!/usr/bin/env python3
"""
Google Ads データ取得→SQLite投入 オーケストレーター

サブエージェントとしてCoworkから呼び出されることを想定。
fetch_and_read() で API データを取得し、GadsDB で SQLite に投入する。

使い方（Coworkのメインエージェントから）:
    import sys
    sys.path.insert(0, "/path/to/GoogleAds_Fetcher")  # trigger_fetch.py がある場所
    sys.path.insert(0, "/path/to/google-ads")          # このファイルがある場所

    from gads_fetcher_agent import run_full_fetch
    from gads_db import set_data_dir

    set_data_dir("/path/to/GoogleAds_Data")
    result = run_full_fetch("065", "2026-02-01", "2026-02-28")

CLI:
    python gads_fetcher_agent.py 065 2026-02-01 2026-02-28
"""

import sys
import json
from pathlib import Path
from datetime import datetime

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from gads_db import GadsDB, set_data_dir
from browser_auction_insight import (
    AuctionInsightConfig,
    build_auction_url,
    get_account_ocid,
    get_ad_groups,
    parse_auction_rows_from_structured,
    build_result,
)
from browser_custom_segment import (
    CustomSegmentConfig,
    prepare_custom_segment_tasks,
    build_audience_manager_url,
    load_latest_audience_data,
    JS_EXTRACT_EXCLUDED_KEYWORDS,
    parse_excluded_from_page_text,
    merge_excluded_keywords,
    save_result as save_excluded_result,
)


# ============================================================
# ログ
# ============================================================

def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ============================================================
# データ取得＋DB投入
# ============================================================

def _fetch(site_id, date_from, date_to, action, campaign=None, label=""):
    """fetch_and_read のラッパー"""
    # trigger_fetch は GoogleAds_Fetcher に存在する前提（sys.path で追加済み）
    from trigger_fetch import fetch_and_read

    desc = label or action
    camp_str = f" (campaign={campaign})" if campaign else ""
    _log(f"▶ {desc}{camp_str} 開始...")

    try:
        data = fetch_and_read(
            site=site_id,
            date_from=date_from,
            date_to=date_to,
            action=action,
            campaign=campaign if campaign else "",
        )
        if data is None:
            _log(f"  ✗ {desc} — データ取得失敗 (None)")
            return None
        rows = data.get("rows", data.get("campaigns", []))
        _log(f"  ✓ {desc} — {len(rows)}行取得")
        return data
    except Exception as e:
        _log(f"  ✗ {desc} — エラー: {e}")
        return None


def run_full_fetch(site_id: str, date_from: str, date_to: str,
                   data_dir: str | Path | None = None) -> dict:
    """
    指定サイトの全データを取得し、SQLiteに投入する。

    Args:
        site_id: サイトID（例: "065"）
        date_from: 開始日（YYYY-MM-DD）
        date_to: 終了日（YYYY-MM-DD）
        data_dir: GoogleAds_Data ディレクトリ（省略時は自動検出）

    Returns:
        {
            "site_id": "065",
            "period": {"from": ..., "to": ...},
            "results": {"campaign_daily": 92, "keyword": 1415, ...},
            "errors": ["fetch_asset_group(...)"],
            "db_path": "/path/to/065.db"
        }
    """
    _log(f"=== サイト {site_id} フルデータ取得＋DB投入 開始 ===")
    _log(f"期間: {date_from} 〜 {date_to}")

    if data_dir:
        set_data_dir(data_dir)

    # DB初期化
    db = GadsDB(site_id, data_dir)
    db.init()
    _log(f"DB初期化完了: {db.db_path}")

    # キャンペーン構成を取得（campaign_db は GoogleAds_Fetcher にある）
    try:
        from campaign_db import list_campaigns
        all_camps = list_campaigns(site_id)
        search_camps = list_campaigns(site_id, campaign_type="検索")
        pmax_camps = list_campaigns(site_id, campaign_type="P-MAX")
        _log(f"キャンペーン: 全{len(all_camps)}件 (検索={len(search_camps)}, Pmax={len(pmax_camps)})")
    except ImportError:
        _log("⚠ campaign_db が見つかりません。全アクションを実行します。")
        search_camps = [{"campaign_id": ""}]  # ダミー
        pmax_camps = []

    results = {}
    errors = []

    # --- 1. キャンペーン日別 ---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_campaign_daily", label="キャンペーン日別")
    if data:
        count = db.import_campaign_daily(data)
        results["campaign_daily"] = count
    else:
        errors.append("campaign_daily")

    # --- 2. 広告グループ日別（検索のみ）---
    if search_camps:
        data = _fetch(site_id, date_from, date_to,
                      "fetch_ad_group_daily", label="広告グループ日別")
        if data:
            count = db.import_ad_group_daily(data)
            results["ad_group_daily"] = count
        else:
            errors.append("ad_group_daily")

    # --- 3. キーワード（検索のみ）---
    if search_camps:
        data = _fetch(site_id, date_from, date_to,
                      "fetch_keyword", label="キーワード")
        if data:
            count = db.import_keyword(data)
            results["keyword"] = count
        else:
            errors.append("keyword")

    # --- 4. 検索語句（検索のみ）---
    if search_camps:
        data = _fetch(site_id, date_from, date_to,
                      "fetch_search_term", label="検索語句")
        if data:
            count = db.import_search_term(data)
            results["search_term"] = count
        else:
            errors.append("search_term")

    # --- 5. ユーザー所在地 ---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_location", label="ユーザー所在地")
    if data:
        count = db.import_location(data)
        results["location"] = count
    else:
        errors.append("location")

    # --- 6. 広告別日別 ---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_ad_daily", label="広告別日別")
    if data:
        count = db.import_ad_daily(data)
        results["ad_daily"] = count
    else:
        errors.append("ad_daily")

    # --- 7. アセットグループ（Pmaxのみ）---
    if pmax_camps:
        total_ag = 0
        for camp in pmax_camps:
            cid = camp["campaign_id"]
            cname = camp.get("campaign_name", cid)
            data = _fetch(site_id, date_from, date_to,
                          "fetch_asset_group", campaign=cid,
                          label=f"アセットグループ ({cname})")
            if data:
                count = db.import_asset_group(data)
                total_ag += count
            else:
                errors.append(f"asset_group({cid})")
        if total_ag > 0:
            results["asset_group"] = total_ag

    # --- 8. 広告アセット（設定スナップショット・日付不要）---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_assets", label="広告アセット")
    if data:
        count = db.import_assets(data)
        results["assets"] = count
    else:
        errors.append("assets")

    # --- 9. アセットの関連付けレポート（期間パフォーマンス）---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_asset_report", label="アセット関連付けレポート")
    if data:
        count = db.import_asset_report(data)
        results["asset_report"] = count
    else:
        errors.append("asset_report")

    # --- 10. 変更履歴（30日窓）---
    data = _fetch(site_id, date_from, date_to,
                  "fetch_change_history", label="変更履歴")
    if data:
        count = db.import_change_history(data)
        results["change_history"] = count
    else:
        errors.append("change_history")

    # --- 結果サマリー ---
    status = db.status()
    db.close()

    total_rows = sum(results.values())
    _log(f"")
    _log(f"=== 完了 ===")
    _log(f"投入行数: {total_rows:,} 行")
    for action, count in results.items():
        _log(f"  {action}: {count:,} 行")
    if errors:
        _log(f"⚠ 失敗: {errors}")
    _log(f"DB: {db.db_path}")

    return {
        "site_id": site_id,
        "period": {"from": date_from, "to": date_to},
        "results": results,
        "errors": errors,
        "db_path": str(db.db_path),
        "db_status": status,
    }


# ============================================================
# ブラウザ経由オークション分析
# ============================================================

def prepare_auction_insight_tasks(
    site_id: str,
    date_from: str,
    date_to: str,
    granularities: list[str] | None = None,
) -> list[dict]:
    """
    ブラウザ経由のオークション分析取得タスク一覧を生成する。

    campaigns.json と browser_config.json から検索キャンペーン・広告グループを
    自動取得し、各組み合わせの AuctionInsightConfig と URL を返す。

    Args:
        site_id: サイトID（例: "065"）
        date_from: 開始日（YYYY-MM-DD）
        date_to: 終了日（YYYY-MM-DD）
        granularities: ["aggregate", "monthly"]（デフォルト: 両方）

    Returns:
        [
            {
                "config": AuctionInsightConfig,
                "url": str,
                "label": str,  # ログ用ラベル
            },
            ...
        ]
    """
    if granularities is None:
        granularities = ["aggregate", "monthly"]

    # browser_config.json から ocid を確認
    ocid = get_account_ocid(site_id)
    if not ocid:
        _log(f"⚠ browser_config.json に site_id={site_id} の ocid が未登録")
        return []

    # campaign_db から検索キャンペーンを取得
    try:
        from campaign_db import list_campaigns
        search_camps = list_campaigns(site_id, campaign_type="検索")
    except ImportError:
        _log("⚠ campaign_db が見つかりません")
        return []

    if not search_camps:
        _log(f"⚠ site_id={site_id} に検索キャンペーンが見つかりません")
        return []

    # accounts.json から customer_id を取得
    try:
        from campaign_db import get_account_info
        account = get_account_info(site_id)
        customer_id = account.get("customer_id", "")
    except (ImportError, Exception):
        customer_id = ""

    tasks = []
    for camp in search_camps:
        cid = camp["campaign_id"]
        cname = camp.get("campaign_name", cid)

        for gran in granularities:
            # キャンペーンレベル
            config = AuctionInsightConfig(
                site_id=site_id,
                customer_id=customer_id,
                campaign_id=cid,
                campaign_name=cname,
                date_from=date_from,
                date_to=date_to,
                granularity=gran,
            )
            tasks.append({
                "config": config,
                "url": build_auction_url(config),
                "label": f"CPN {cname} ({gran})",
            })

            # 広告グループレベル
            ad_groups = get_ad_groups(site_id, cid)
            for ag in ad_groups:
                ag_config = AuctionInsightConfig(
                    site_id=site_id,
                    customer_id=customer_id,
                    campaign_id=cid,
                    campaign_name=cname,
                    date_from=date_from,
                    date_to=date_to,
                    ad_group_id=ag["ad_group_id"],
                    ad_group_name=ag["ad_group_name"],
                    granularity=gran,
                )
                tasks.append({
                    "config": ag_config,
                    "url": build_auction_url(ag_config),
                    "label": f"AG {ag['ad_group_name']} ({gran})",
                })

    _log(f"オークション分析タスク: {len(tasks)}件 生成")
    for t in tasks:
        _log(f"  - {t['label']}")
    return tasks


def import_browser_auction_data(
    site_id: str,
    config: AuctionInsightConfig,
    elements: list[dict],
    data_dir: str | Path | None = None,
    year_hint: str = "2026",
) -> int:
    """
    ブラウザから抽出したオークション分析データをDBに投入する。

    サブエージェントが Chrome MCP の javascript_tool で取得した
    テーブルデータ（elements）を受け取り、パース→DB投入する。

    Args:
        site_id: サイトID
        config: AuctionInsightConfig（該当の設定）
        elements: [{"domain": "...", "impression_share": "62.49%", ...}, ...]
        data_dir: GoogleAds_Data ディレクトリ（省略時は自動検出）
        year_hint: 月別データの年

    Returns:
        投入行数
    """
    if data_dir:
        set_data_dir(data_dir)

    db = GadsDB(site_id, data_dir)
    db.init()

    rows = parse_auction_rows_from_structured(elements, config, year_hint)
    result = build_result(config, rows)
    count = db.import_auction_insight_browser(result)
    db.close()

    _log(f"  ✓ オークション分析DB投入: {count}行 ({config.campaign_name})")
    return count


# ============================================================
# カスタムセグメント除外キーワード取得（API + ブラウザ）
# ============================================================

def fetch_audience_and_prepare_browser_tasks(
    site_id: str,
    data_dir: str | Path | None = None,
    skip_api: bool = False,
) -> dict:
    """
    カスタムセグメント除外キーワード取得の統合フロー（ステップ1-2）。

    1. API で audience_settings を取得（skip_api=True で既存データを使用）
    2. 検索kwdタイプのセグメントからブラウザ確認タスクリストを生成

    Args:
        site_id: サイトID (例: "072")
        data_dir: GoogleAds_Data ディレクトリ（省略時は自動検出）
        skip_api: True の場合、API取得をスキップして既存データを使用

    Returns:
        {
            "audience_data": dict,      # API取得データ
            "tasks": list[dict],        # ブラウザ確認タスクリスト
            "manager_url": str,         # オーディエンスマネージャーURL
            "segment_names": dict,      # {segment_id: segment_name} マッピング
        }
    """
    # ステップ1: API取得
    if not skip_api:
        _log(f"[{site_id}] API で audience_settings を取得中...")
        api_data = _fetch(site_id, "", "", "fetch_audience_settings",
                          label="audience_settings")
        if api_data is None:
            _log(f"⚠ API取得失敗。既存データを使用します。")
            skip_api = True

    # 既存データを読み込む
    audience_data = load_latest_audience_data(site_id, data_dir)
    _log(f"[{site_id}] audience_data 読み込み完了")

    custom_segments = audience_data.get("custom_segments", [])
    search_kwd = [s for s in custom_segments if s.get("member_type") == "検索キーワード"]
    _log(f"  カスタムセグメント: {len(custom_segments)}件（うち検索kwd: {len(search_kwd)}件）")

    # ステップ2: タスク生成
    tasks = prepare_custom_segment_tasks(site_id, audience_data)
    _log(f"  ブラウザ確認タスク: {len(tasks)}件")

    manager_url = build_audience_manager_url(tasks[0]["config"].ocid) if tasks else ""

    segment_names = {
        t["config"].segment_id: t["config"].segment_name
        for t in tasks
    }

    for t in tasks:
        _log(f"    - {t['label']}")

    return {
        "audience_data": audience_data,
        "tasks": tasks,
        "manager_url": manager_url,
        "segment_names": segment_names,
    }


def finalize_excluded_keywords(
    site_id: str,
    audience_data: dict,
    excluded_map: dict[str, list[str]],
    data_dir: str | Path | None = None,
) -> dict:
    """
    ブラウザ取得した除外キーワードをAPIデータと統合してJSON保存する（ステップ4）。

    Args:
        site_id: サイトID
        audience_data: fetch_audience_settings で取得した JSON データ
        excluded_map: {segment_id: [excluded_keyword, ...]}
        data_dir: 保存先ディレクトリ

    Returns:
        {
            "filepath": Path,
            "summary": dict,
            "result": dict,
        }
    """
    _log(f"[{site_id}] 除外キーワードを統合中...")

    result = merge_excluded_keywords(audience_data, excluded_map)
    filepath = save_excluded_result(result, data_dir)

    summary = result["summary"]
    _log(f"  保存先: {filepath}")
    _log(f"  セグメント数: {summary['total_segments']}")
    _log(f"  除外ありセグメント: {summary['segments_with_excluded']}")
    _log(f"  除外なしセグメント: {summary['segments_without_excluded']}")
    _log(f"  除外キーワード合計: {summary['total_excluded_keywords']}")

    return {
        "filepath": filepath,
        "summary": summary,
        "result": result,
    }


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python gads_fetcher_agent.py <site_id> <date_from> <date_to> [--data-dir PATH]")
        print("Example: python gads_fetcher_agent.py 065 2026-02-01 2026-02-28")
        sys.exit(1)

    _site_id = sys.argv[1]
    _date_from = sys.argv[2]
    _date_to = sys.argv[3]

    _data_dir = None
    if "--data-dir" in sys.argv:
        idx = sys.argv.index("--data-dir")
        if idx + 1 < len(sys.argv):
            _data_dir = sys.argv[idx + 1]

    result = run_full_fetch(_site_id, _date_from, _date_to, _data_dir)

    if result["errors"]:
        print(f"\n⚠ 失敗したデータ種類: {result['errors']}")
