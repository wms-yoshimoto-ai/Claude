#!/usr/bin/env python3
"""
Google Ads アセットの関連付けレポート取得スクリプト
管理画面「アセットの関連付けレポート」と同じ列構成・集計形式で出力する

【参考CSV列】
  アセットのステータス, アセット, アセットタイプ, キャンペーン, 広告グループ,
  レベル, ステータス, ステータスの理由, 最終更新日,
  クリック数, 表示回数, クリック率, 通貨コード, 平均クリック単価,
  費用, コンバージョン, すべてのコンバージョン,
  キャンペーン ID, 広告グループ ID, アイテム ID

【使い方】
  # 全レベル・月別指定
  python3 fetch_asset_report.py --site 065 --month 202601

  # 日付範囲指定
  python3 fetch_asset_report.py --site 065 --from 2026-01-01 --to 2026-01-31

  # キャンペーンレベルのみ
  python3 fetch_asset_report.py --site 065 --month 202601 --level campaign

  # 広告グループレベルのみ
  python3 fetch_asset_report.py --site 065 --month 202601 --level ad_group

  # アカウントレベルのみ
  python3 fetch_asset_report.py --site 065 --month 202601 --level account

  # 特定キャンペーンのみ
  python3 fetch_asset_report.py --site 065 --month 202601 --campaign 23335615195

【取得レベル】
  - campaign  : campaign_asset（キャンペーンレベル）
  - ad_group  : ad_group_asset（広告グループレベル）
  - account   : customer_asset（アカウントレベル）
  - all       : 上記3レベル全て（デフォルト）

【取得アセットタイプ】
  - サイトリンク（SITELINK）
  - コールアウト（CALLOUT）
  - 構造化スニペット（STRUCTURED_SNIPPET）
  - 画像（MARKETING_IMAGE / SQUARE_MARKETING_IMAGE等）
  ※ 電話番号（CALL）は除外

【出力ファイル】
  {site_id}_asset_report_{level}_{date_from}_{date_to}.json
  {site_id}_asset_report_{level}_{date_from}_{date_to}.csv

【注意事項】
  - 期間合計（日次内訳なし）: segments.date は WHERE 句のみに使用
  - cost は micros → JPY に変換して整数で出力
  - クリック率・平均クリック単価はスクリプト内で計算して出力
  - アカウントレベル（customer_asset）は --campaign フィルタ非対応
"""

import json
import sys
import csv
import argparse
import calendar
import requests
from datetime import datetime
from pathlib import Path
from campaign_db import resolve_campaign_id

# ============================================================
# パス設定
# ============================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"

# ============================================================
# アセットタイプ設定
# ============================================================

IMAGE_FIELD_TYPES = [
    "MARKETING_IMAGE",
    "SQUARE_MARKETING_IMAGE",
    "PORTRAIT_MARKETING_IMAGE",
    "LOGO",
    "LANDSCAPE_LOGO",
]

TARGET_FIELD_TYPES = (
    ["SITELINK", "CALLOUT", "STRUCTURED_SNIPPET"]
    + IMAGE_FIELD_TYPES
)

FIELD_TYPE_TO_JA = {
    "SITELINK":              "サイトリンク",
    "CALLOUT":               "コールアウト",
    "STRUCTURED_SNIPPET":    "構造化スニペット",
    "MARKETING_IMAGE":       "画像",
    "SQUARE_MARKETING_IMAGE":"画像",
    "PORTRAIT_MARKETING_IMAGE":"画像",
    "LOGO":                  "画像",
    "LANDSCAPE_LOGO":        "画像",
}

STATUS_TO_JA = {
    "ENABLED":  "有効",
    "PAUSED":   "一時停止",
    "REMOVED":  "削除済み",
    "ENABLED_AND_ACTIVE": "有効",
}

APPROVAL_TO_JA = {
    "APPROVED":          "有効",
    "APPROVED_LIMITED":  "制限付き",
    "AREA_OF_INTEREST_ONLY": "対象地域限定",
    "DISAPPROVED":       "不承認",
    "UNKNOWN":           "",
    "UNSPECIFIED":       "",
}

REVIEW_TO_JA = {
    "APPROVED":             "",
    "APPROVED_LIMITED":     "承認済み（制限付き）",
    "DISAPPROVED":          "不承認",
    "UNDER_REVIEW":         "審査中",
    "ELIGIBLE_MAY_SERVE":   "配信可能",
    "UNKNOWN":              "",
    "UNSPECIFIED":          "",
}

# ============================================================
# 管理画面互換 CSV 列名
# ============================================================

CSV_COLUMNS_JA = [
    "アセットのステータス",
    "アセット",
    "アセットタイプ",
    "キャンペーン",
    "広告グループ",
    "レベル",
    "ステータス",
    "ステータスの理由",
    "最終更新日",
    "クリック数",
    "表示回数",
    "クリック率",
    "通貨コード",
    "平均クリック単価",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "キャンペーン ID",
    "広告グループ ID",
    "アイテム ID",
]

# ============================================================
# 認証・アカウント情報
# ============================================================

def load_credentials():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)["google_ads"]

def load_account(site_id: str) -> dict:
    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)["accounts"]
    acct = next((a for a in accounts if a.get("site_id") == site_id), None)
    if not acct:
        raise ValueError(f"サイトID '{site_id}' が accounts.json に見つかりません")
    return acct

def get_access_token(creds: dict) -> str:
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     creds["oauth"]["client_id"],
            "client_secret": creds["oauth"]["client_secret"],
            "refresh_token": creds["oauth"]["refresh_token"],
            "grant_type":    "refresh_token",
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def gaql_request(customer_id: str, gaql: str, creds: dict, token: str) -> list:
    """GAQL クエリを実行してレスポンスのリストを返す"""
    url = f"https://googleads.googleapis.com/v22/customers/{customer_id}/googleAds:searchStream"
    headers = {
        "Authorization":     f"Bearer {token}",
        "developer-token":   creds["developer_token"],
        "login-customer-id": creds["mcc_customer_id"],
        "Content-Type":      "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": gaql})
    if resp.status_code != 200:
        print(f"[ERROR] GAQL失敗: {resp.status_code}", file=sys.stderr)
        print(resp.text[:500], file=sys.stderr)
        return []
    results = []
    try:
        data = json.loads(resp.text)
    except json.JSONDecodeError:
        print(f"[ERROR] JSONパース失敗", file=sys.stderr)
        return []
    # searchStream のレスポンスは配列
    if isinstance(data, list):
        for batch in data:
            if isinstance(batch, dict):
                results.extend(batch.get("results", []))
    elif isinstance(data, dict):
        results.extend(data.get("results", []))
    return results

# ============================================================
# 共通ヘルパー
# ============================================================

def format_asset_text(asset: dict, field_type: str) -> str:
    """アセット内容を管理画面の「アセット」列と同じ形式に整形"""
    if field_type == "SITELINK":
        sl = asset.get("sitelinkAsset", {})
        parts = [sl.get("linkText", "")]
        if sl.get("description1"):
            parts.append(sl["description1"])
        if sl.get("description2"):
            parts.append(sl["description2"])
        final_urls = asset.get("finalUrls", sl.get("finalUrls", []))
        if final_urls:
            parts.append(final_urls[0])
        return "\n".join(parts)
    elif field_type == "CALLOUT":
        return asset.get("calloutAsset", {}).get("calloutText", "")
    elif field_type == "STRUCTURED_SNIPPET":
        sn     = asset.get("structuredSnippetAsset", {})
        header = sn.get("header", "")
        values = sn.get("values", [])
        return f"{header}:{','.join(values)}" if values else header
    elif field_type in IMAGE_FIELD_TYPES:
        img = asset.get("imageAsset", {})
        return img.get("fullSize", {}).get("url", "")
    return ""

def calc_ctr(impressions, clicks) -> str:
    """クリック率を文字列で返す（0件時は ' --'）"""
    try:
        imp = int(impressions)
        clk = int(clicks)
        if imp == 0:
            return " --"
        return f"{clk / imp * 100:.2f}%"
    except (TypeError, ValueError):
        return " --"

def calc_avg_cpc(clicks, cost) -> int:
    """平均クリック単価を整数で返す（0クリック時は 0）"""
    try:
        clk  = int(clicks)
        cst  = float(cost)
        if clk == 0:
            return 0
        return round(cst / clk)
    except (TypeError, ValueError):
        return 0

def parse_metrics(metrics: dict) -> dict:
    """metrics を整理して返す（cost は JPY 整数）"""
    cost_micros = metrics.get("costMicros", 0)
    try:
        cost = round(int(cost_micros) / 1_000_000)
    except (TypeError, ValueError):
        cost = 0
    return {
        "impressions":     int(metrics.get("impressions", 0) or 0),
        "clicks":          int(metrics.get("clicks", 0) or 0),
        "cost":            cost,
        "conversions":     float(metrics.get("conversions", 0) or 0),
        "all_conversions": float(metrics.get("allConversions", 0) or 0),
    }

def build_row(
    asset:          dict,
    field_type:     str,
    assoc_status:   str,   # ENABLED / PAUSED / REMOVED
    campaign_id:    str,
    campaign_name:  str,
    ad_group_id:    str,
    ad_group_name:  str,
    level_ja:       str,   # キャンペーン / 広告グループ / アカウント
    metrics:        dict,
) -> dict:
    """共通行データを構築（JSON 用 + CSV 用どちらにも使える）"""
    m          = parse_metrics(metrics)
    asset_text = format_asset_text(asset, field_type)
    type_ja    = FIELD_TYPE_TO_JA.get(field_type, field_type)
    status_ja  = STATUS_TO_JA.get(assoc_status, assoc_status)

    # policy ステータス
    policy = asset.get("policySummary", {})
    approval_status = policy.get("approvalStatus", "")
    review_status   = policy.get("reviewStatus", "")
    policy_ja       = APPROVAL_TO_JA.get(approval_status, "")
    reason_ja       = REVIEW_TO_JA.get(review_status, "")

    # 表示ステータス = policy が取れればそちらを、なければ assoc_status
    display_status = policy_ja if policy_ja else status_ja

    ctr     = calc_ctr(m["impressions"], m["clicks"])
    avg_cpc = calc_avg_cpc(m["clicks"], m["cost"])

    return {
        # --- CSV (管理画面互換) ---
        "アセットのステータス": status_ja,
        "アセット":          asset_text,
        "アセットタイプ":     type_ja,
        "キャンペーン":       campaign_name,
        "広告グループ":       ad_group_name,
        "レベル":             level_ja,
        "ステータス":         display_status,
        "ステータスの理由":   reason_ja,
        "最終更新日":         "",   # API では直接取得不可のため空欄
        "クリック数":         m["clicks"],
        "表示回数":           m["impressions"],
        "クリック率":         ctr,
        "通貨コード":         "JPY",
        "平均クリック単価":   avg_cpc,
        "費用":               m["cost"],
        "コンバージョン":     m["conversions"],
        "すべてのコンバージョン": m["all_conversions"],
        "キャンペーン ID":    campaign_id,
        "広告グループ ID":    ad_group_id,
        "アイテム ID":        str(asset.get("id", "")),
        # --- JSON 追加フィールド（英語）---
        "_level_en":        "CAMPAIGN" if level_ja == "キャンペーン"
                            else ("AD_GROUP" if level_ja == "広告グループ" else "ACCOUNT"),
        "_asset_field_type": field_type,
        "_asset_name":      asset.get("name", ""),
        "_campaign_id":     campaign_id,
        "_ad_group_id":     ad_group_id,
    }

# ============================================================
# GAQL SELECT 共通部品（アセットコンテンツ）
# ============================================================

ASSET_CONTENT_SELECT = """
            asset.id,
            asset.name,
            asset.policy_summary.approval_status,
            asset.policy_summary.review_status,
            asset.sitelink_asset.link_text,
            asset.sitelink_asset.description1,
            asset.sitelink_asset.description2,
            asset.final_urls,
            asset.callout_asset.callout_text,
            asset.structured_snippet_asset.header,
            asset.structured_snippet_asset.values,
            asset.image_asset.full_size.url,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions
"""

# ============================================================
# データ取得：キャンペーンレベル
# ============================================================

def fetch_campaign_level(
    customer_id: str, date_from: str, date_to: str,
    campaign_filter: str, creds: dict, token: str,
) -> list:
    field_types_str = ", ".join(f"'{ft}'" for ft in TARGET_FIELD_TYPES)
    gaql = f"""
        SELECT
            campaign_asset.field_type,
            campaign_asset.status,
            campaign.id,
            campaign.name,
            campaign.status,
            {ASSET_CONTENT_SELECT}
        FROM campaign_asset
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign_asset.field_type IN ({field_types_str})
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, campaign_asset.field_type, asset.id
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset      = r.get("asset", {})
        ca         = r.get("campaignAsset", {})
        cpn        = r.get("campaign", {})
        row = build_row(
            asset         = asset,
            field_type    = ca.get("fieldType", ""),
            assoc_status  = ca.get("status", ""),
            campaign_id   = str(cpn.get("id", "")),
            campaign_name = cpn.get("name", ""),
            ad_group_id   = "",
            ad_group_name = "",
            level_ja      = "キャンペーン",
            metrics       = r.get("metrics", {}),
        )
        rows.append(row)
    return rows


# ============================================================
# データ取得：広告グループレベル
# ============================================================

def fetch_ad_group_level(
    customer_id: str, date_from: str, date_to: str,
    campaign_filter: str, creds: dict, token: str,
) -> list:
    field_types_str = ", ".join(f"'{ft}'" for ft in TARGET_FIELD_TYPES)
    gaql = f"""
        SELECT
            ad_group_asset.field_type,
            ad_group_asset.status,
            campaign.id,
            campaign.name,
            campaign.status,
            ad_group.id,
            ad_group.name,
            {ASSET_CONTENT_SELECT}
        FROM ad_group_asset
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND ad_group_asset.field_type IN ({field_types_str})
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, ad_group.name, ad_group_asset.field_type, asset.id
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset      = r.get("asset", {})
        aga        = r.get("adGroupAsset", {})
        cpn        = r.get("campaign", {})
        adg        = r.get("adGroup", {})
        row = build_row(
            asset         = asset,
            field_type    = aga.get("fieldType", ""),
            assoc_status  = aga.get("status", ""),
            campaign_id   = str(cpn.get("id", "")),
            campaign_name = cpn.get("name", ""),
            ad_group_id   = str(adg.get("id", "")),
            ad_group_name = adg.get("name", ""),
            level_ja      = "広告グループ",
            metrics       = r.get("metrics", {}),
        )
        rows.append(row)
    return rows


# ============================================================
# データ取得：アカウントレベル（customer_asset）
# ============================================================

def fetch_account_level(
    customer_id: str, date_from: str, date_to: str,
    creds: dict, token: str,
) -> list:
    """customer_asset でアカウントレベルのアセットを取得"""
    field_types_str = ", ".join(f"'{ft}'" for ft in TARGET_FIELD_TYPES)
    gaql = f"""
        SELECT
            customer_asset.field_type,
            customer_asset.status,
            {ASSET_CONTENT_SELECT}
        FROM customer_asset
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND customer_asset.field_type IN ({field_types_str})
        ORDER BY customer_asset.field_type, asset.id
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        cusa  = r.get("customerAsset", {})
        row = build_row(
            asset         = asset,
            field_type    = cusa.get("fieldType", ""),
            assoc_status  = cusa.get("status", ""),
            campaign_id   = "",
            campaign_name = "",
            ad_group_id   = "",
            ad_group_name = "",
            level_ja      = "アカウント",
            metrics       = r.get("metrics", {}),
        )
        rows.append(row)
    return rows


# ============================================================
# JSON / CSV 出力
# ============================================================

def save_json(rows: list, site_id: str, level: str,
              date_from: str, date_to: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{site_id}_asset_report_{level}_{date_from}_{date_to}.json"
    path  = OUTPUT_DIR / fname

    from collections import Counter
    type_count  = Counter(r["アセットタイプ"] for r in rows)
    level_count = Counter(r["レベル"]        for r in rows)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "site_id":    site_id,
            "period":     {"from": date_from, "to": date_to},
            "level":      level,
            "fetched_at": datetime.now().isoformat(),
            "total_rows": len(rows),
            "summary": {
                "by_asset_type": dict(type_count),
                "by_level":      dict(level_count),
            },
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"[JSON] {path}")
    return path


def save_csv(rows: list, site_id: str, level: str,
             date_from: str, date_to: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{site_id}_asset_report_{level}_{date_from}_{date_to}.csv"
    path  = OUTPUT_DIR / fname

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_COLUMNS_JA, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"[CSV] {path}")
    return path


# ============================================================
# 期間ヘルパー
# ============================================================

def resolve_period(args) -> tuple:
    if args.month:
        try:
            year  = int(args.month[:4])
            month = int(args.month[4:])
            last_day  = calendar.monthrange(year, month)[1]
            date_from = f"{year:04d}-{month:02d}-01"
            date_to   = f"{year:04d}-{month:02d}-{last_day:02d}"
        except (ValueError, IndexError):
            print(f"[ERROR] --month の形式が不正です（YYYYMM 例: 202601）: {args.month}",
                  file=sys.stderr)
            sys.exit(1)
    elif args.date_from and args.date_to:
        date_from = args.date_from
        date_to   = args.date_to
    else:
        print("[ERROR] --month または --from / --to を指定してください", file=sys.stderr)
        sys.exit(1)
    return date_from, date_to


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads アセットの関連付けレポート取得")
    parser.add_argument("--site",     required=True,
                        help="サイトID（例: 065）")
    parser.add_argument("--level",    default="all",
                        choices=["campaign", "ad_group", "account", "all"],
                        help="取得レベル（campaign / ad_group / account / all）")
    parser.add_argument("--month",    default="",
                        help="月別指定（YYYYMM 例: 202601）")
    parser.add_argument("--from",     dest="date_from", default="",
                        help="開始日（YYYY-MM-DD）")
    parser.add_argument("--to",       dest="date_to",   default="",
                        help="終了日（YYYY-MM-DD）")
    parser.add_argument("--campaign", default="",
                        help="キャンペーンID（省略時は全件）")
    args = parser.parse_args()

    # ── 期間解決 ──
    date_from, date_to = resolve_period(args)

    # ── アカウント情報の読み込み ──
    creds = load_credentials()
    acct  = load_account(args.site)
    cid   = acct["customer_id"].replace("-", "")
    token = get_access_token(creds)

    # ── キャンペーンIDの解決 ──
    campaign_id = ""
    if args.campaign:
        campaign_id = resolve_campaign_id(args.campaign, args.site)
    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""

    # ── レベル決定 ──
    levels = (
        ["campaign", "ad_group", "account"] if args.level == "all"
        else [args.level]
    )

    print(f"[INFO] サイト: {args.site} / 顧客ID: {cid}")
    print(f"[INFO] 期間: {date_from} ～ {date_to}")
    print(f"[INFO] レベル: {', '.join(levels)}")
    if campaign_id:
        print(f"[INFO] キャンペーンフィルタ: {campaign_id}")

    all_rows = []

    # ── 各レベルを取得 ──
    if "campaign" in levels:
        print("[INFO] キャンペーンレベルを取得中（campaign_asset）...")
        rows = fetch_campaign_level(cid, date_from, date_to, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "ad_group" in levels:
        print("[INFO] 広告グループレベルを取得中（ad_group_asset）...")
        rows = fetch_ad_group_level(cid, date_from, date_to, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "account" in levels:
        print("[INFO] アカウントレベルを取得中（customer_asset）...")
        if campaign_id:
            print("  [WARN] アカウントレベルは --campaign フィルタ非対応のため全件取得します")
        rows = fetch_account_level(cid, date_from, date_to, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    print(f"[INFO] 合計: {len(all_rows)} 件")

    # ── 出力 ──
    if all_rows:
        save_json(all_rows, args.site, args.level, date_from, date_to)
        save_csv(all_rows,  args.site, args.level, date_from, date_to)
    else:
        print("[WARN] 取得データが0件でした")

    print("[INFO] 完了")


if __name__ == "__main__":
    main()
