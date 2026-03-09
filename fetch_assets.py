#!/usr/bin/env python3
"""
Google Ads 広告アセット取得スクリプト
サイトリンク / コールアウト / 構造化スニペット / 電話番号 / 画像アセット の設定内容を取得する

【使い方】
  # 全アセットを取得
  python3 fetch_assets.py --site 065

  # 特定キャンペーンのみ
  python3 fetch_assets.py --site 065 --campaign 23335615195

  # 特定タイプのみ（カンマ区切り）
  python3 fetch_assets.py --site 065 --types sitelink,callout

【取得対象】
  - SITELINK（サイトリンク）: テキスト、説明文1・2、最終ページURL
  - CALLOUT（コールアウト）: テキスト
  - STRUCTURED_SNIPPET（構造化スニペット）: ヘッダー、値リスト
  - CALL（電話番号）: 電話番号、国コード
  - IMAGE（画像アセット）: URL、サイズ、MIMEタイプ

【出力ファイル】
  {site_id}_assets_{timestamp}.json
  {site_id}_assets_{timestamp}.csv  （人間が見やすいCSV）

【注意事項】
  - 日付指定不要（現在の設定スナップショットを取得）
  - パフォーマンス指標（imp/clk等）は取得しない（設定情報のみ）
  - アカウントレベルのアセット（customer_asset）も取得対象
  - キャンペーンレベルとアカウントレベルの両方を取得し level カラムで区別
"""

import json
import sys
import csv
import argparse
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

# 取得対象アセットタイプ
ASSET_TYPES_ALL = ["sitelink", "callout", "structured_snippet", "call", "image"]

# 画像アセットのfield_type一覧
IMAGE_FIELD_TYPES = [
    "MARKETING_IMAGE",
    "SQUARE_MARKETING_IMAGE",
    "PORTRAIT_MARKETING_IMAGE",
    "LOGO",
    "LANDSCAPE_LOGO",
]

# CSV出力列（全アセット共通＋タイプ別カラムは後半）
CSV_COLUMNS = [
    "asset_type",
    "asset_id",
    "asset_name",
    "level",           # CAMPAIGN / ACCOUNT
    "campaign_id",
    "campaign_name",
    "status",
    # Sitelink
    "sitelink_text",
    "sitelink_desc1",
    "sitelink_desc2",
    "sitelink_url",
    # Callout
    "callout_text",
    # Structured Snippet
    "snippet_header",
    "snippet_values",
    # Call
    "call_phone",
    "call_country",
    # Image
    "image_field_type",
    "image_url",
    "image_width",
    "image_height",
    "image_mime_type",
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
        "login-customer-id": creds.get("manager_customer_id", ""),
        "Content-Type":      "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": gaql})
    if resp.status_code != 200:
        print(f"[ERROR] GAQL失敗: {resp.status_code}", file=sys.stderr)
        print(resp.text[:500], file=sys.stderr)
        return []
    results = []
    for line in resp.text.strip().splitlines():
        try:
            batch = json.loads(line)
            results.extend(batch.get("results", []))
        except json.JSONDecodeError:
            pass
    return results

# ============================================================
# データ取得：キャンペーンレベル
# ============================================================

def fetch_sitelinks(customer_id: str, campaign_filter: str,
                    creds: dict, token: str) -> list:
    """サイトリンクをキャンペーンレベルで取得"""
    gaql = f"""
        SELECT
            asset.id,
            asset.name,
            asset.type,
            asset.sitelink_asset.link_text,
            asset.sitelink_asset.description1,
            asset.sitelink_asset.description2,
            asset.sitelink_asset.final_urls,
            campaign_asset.status,
            campaign_asset.asset_field_type,
            campaign.id,
            campaign.name
        FROM campaign_asset
        WHERE campaign_asset.asset_field_type = 'SITELINK'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, asset.sitelink_asset.link_text
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        sl    = asset.get("sitelinkAsset", {})
        cpn   = r.get("campaign", {})
        ca    = r.get("campaignAsset", {})
        final_urls = sl.get("finalUrls", [])
        rows.append({
            "asset_type":    "SITELINK",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "CAMPAIGN",
            "campaign_id":   str(cpn.get("id", "")),
            "campaign_name": cpn.get("name", ""),
            "status":        ca.get("status", ""),
            "sitelink_text": sl.get("linkText", ""),
            "sitelink_desc1": sl.get("description1", ""),
            "sitelink_desc2": sl.get("description2", ""),
            "sitelink_url":  final_urls[0] if final_urls else "",
            "callout_text":  None,
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_callouts(customer_id: str, campaign_filter: str,
                   creds: dict, token: str) -> list:
    """コールアウトをキャンペーンレベルで取得"""
    gaql = f"""
        SELECT
            asset.id,
            asset.name,
            asset.callout_asset.callout_text,
            campaign_asset.status,
            campaign.id,
            campaign.name
        FROM campaign_asset
        WHERE campaign_asset.asset_field_type = 'CALLOUT'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, asset.callout_asset.callout_text
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        co    = asset.get("calloutAsset", {})
        cpn   = r.get("campaign", {})
        ca    = r.get("campaignAsset", {})
        rows.append({
            "asset_type":    "CALLOUT",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "CAMPAIGN",
            "campaign_id":   str(cpn.get("id", "")),
            "campaign_name": cpn.get("name", ""),
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  co.get("calloutText", ""),
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_structured_snippets(customer_id: str, campaign_filter: str,
                               creds: dict, token: str) -> list:
    """構造化スニペットをキャンペーンレベルで取得"""
    gaql = f"""
        SELECT
            asset.id,
            asset.name,
            asset.structured_snippet_asset.header,
            asset.structured_snippet_asset.values,
            campaign_asset.status,
            campaign.id,
            campaign.name
        FROM campaign_asset
        WHERE campaign_asset.asset_field_type = 'STRUCTURED_SNIPPET'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, asset.structured_snippet_asset.header
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        sn    = asset.get("structuredSnippetAsset", {})
        cpn   = r.get("campaign", {})
        ca    = r.get("campaignAsset", {})
        values = sn.get("values", [])
        rows.append({
            "asset_type":    "STRUCTURED_SNIPPET",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "CAMPAIGN",
            "campaign_id":   str(cpn.get("id", "")),
            "campaign_name": cpn.get("name", ""),
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  None,
            "snippet_header": sn.get("header", ""),
            "snippet_values": json.dumps(values, ensure_ascii=False) if values else "",
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_call_assets(customer_id: str, campaign_filter: str,
                      creds: dict, token: str) -> list:
    """電話番号アセットをキャンペーンレベルで取得"""
    gaql = f"""
        SELECT
            asset.id,
            asset.name,
            asset.call_asset.phone_number,
            asset.call_asset.country_code,
            asset.call_asset.call_conversion_reporting_state,
            campaign_asset.status,
            campaign.id,
            campaign.name
        FROM campaign_asset
        WHERE campaign_asset.asset_field_type = 'CALL'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        ca_data = asset.get("callAsset", {})
        cpn   = r.get("campaign", {})
        ca    = r.get("campaignAsset", {})
        rows.append({
            "asset_type":    "CALL",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "CAMPAIGN",
            "campaign_id":   str(cpn.get("id", "")),
            "campaign_name": cpn.get("name", ""),
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  None,
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    ca_data.get("phoneNumber", ""),
            "call_country":  ca_data.get("countryCode", ""),
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_image_assets(customer_id: str, campaign_filter: str,
                       creds: dict, token: str) -> list:
    """画像アセットをキャンペーンレベルで取得（複数field_type対応）"""
    field_types_str = ", ".join(f"'{ft}'" for ft in IMAGE_FIELD_TYPES)
    gaql = f"""
        SELECT
            asset.id,
            asset.name,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.full_size.height_pixels,
            asset.image_asset.mime_type,
            campaign_asset.status,
            campaign_asset.asset_field_type,
            campaign.id,
            campaign.name
        FROM campaign_asset
        WHERE campaign_asset.asset_field_type IN ({field_types_str})
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, campaign_asset.asset_field_type
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset    = r.get("asset", {})
        img      = asset.get("imageAsset", {})
        full_sz  = img.get("fullSize", {})
        cpn      = r.get("campaign", {})
        ca       = r.get("campaignAsset", {})
        rows.append({
            "asset_type":    "IMAGE",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "CAMPAIGN",
            "campaign_id":   str(cpn.get("id", "")),
            "campaign_name": cpn.get("name", ""),
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  None,
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": ca.get("assetFieldType", ""),
            "image_url":     full_sz.get("url", ""),
            "image_width":   full_sz.get("widthPixels"),
            "image_height":  full_sz.get("heightPixels"),
            "image_mime_type": img.get("mimeType", ""),
        })
    return rows

# ============================================================
# データ取得：アカウントレベル（customer_asset）
# ============================================================

def fetch_account_sitelinks(customer_id: str, creds: dict, token: str) -> list:
    gaql = """
        SELECT
            asset.id,
            asset.name,
            asset.sitelink_asset.link_text,
            asset.sitelink_asset.description1,
            asset.sitelink_asset.description2,
            asset.sitelink_asset.final_urls,
            customer_asset.status
        FROM customer_asset
        WHERE customer_asset.asset_field_type = 'SITELINK'
        ORDER BY asset.sitelink_asset.link_text
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        sl    = asset.get("sitelinkAsset", {})
        ca    = r.get("customerAsset", {})
        final_urls = sl.get("finalUrls", [])
        rows.append({
            "asset_type":    "SITELINK",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "ACCOUNT",
            "campaign_id":   "",
            "campaign_name": "",
            "status":        ca.get("status", ""),
            "sitelink_text": sl.get("linkText", ""),
            "sitelink_desc1": sl.get("description1", ""),
            "sitelink_desc2": sl.get("description2", ""),
            "sitelink_url":  final_urls[0] if final_urls else "",
            "callout_text":  None,
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_account_callouts(customer_id: str, creds: dict, token: str) -> list:
    gaql = """
        SELECT
            asset.id,
            asset.name,
            asset.callout_asset.callout_text,
            customer_asset.status
        FROM customer_asset
        WHERE customer_asset.asset_field_type = 'CALLOUT'
        ORDER BY asset.callout_asset.callout_text
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset = r.get("asset", {})
        co    = asset.get("calloutAsset", {})
        ca    = r.get("customerAsset", {})
        rows.append({
            "asset_type":    "CALLOUT",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "ACCOUNT",
            "campaign_id":   "",
            "campaign_name": "",
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  co.get("calloutText", ""),
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    None,
            "call_country":  None,
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows


def fetch_account_call(customer_id: str, creds: dict, token: str) -> list:
    gaql = """
        SELECT
            asset.id,
            asset.name,
            asset.call_asset.phone_number,
            asset.call_asset.country_code,
            customer_asset.status
        FROM customer_asset
        WHERE customer_asset.asset_field_type = 'CALL'
        ORDER BY asset.call_asset.phone_number
    """
    results = gaql_request(customer_id, gaql, creds, token)
    rows = []
    for r in results:
        asset   = r.get("asset", {})
        ca_data = asset.get("callAsset", {})
        ca      = r.get("customerAsset", {})
        rows.append({
            "asset_type":    "CALL",
            "asset_id":      str(asset.get("id", "")),
            "asset_name":    asset.get("name", ""),
            "level":         "ACCOUNT",
            "campaign_id":   "",
            "campaign_name": "",
            "status":        ca.get("status", ""),
            "sitelink_text": None,
            "sitelink_desc1": None,
            "sitelink_desc2": None,
            "sitelink_url":  None,
            "callout_text":  None,
            "snippet_header": None,
            "snippet_values": None,
            "call_phone":    ca_data.get("phoneNumber", ""),
            "call_country":  ca_data.get("countryCode", ""),
            "image_field_type": None,
            "image_url":     None,
            "image_width":   None,
            "image_height":  None,
            "image_mime_type": None,
        })
    return rows

# ============================================================
# JSON / CSV 出力
# ============================================================

def save_json(rows: list, site_id: str, ts: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{site_id}_assets_{ts}.json"

    # サマリー集計
    from collections import Counter
    type_count = Counter(r["asset_type"] for r in rows)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "site_id":    site_id,
            "fetched_at": datetime.now().isoformat(),
            "total_rows": len(rows),
            "summary": {
                "sitelink":           type_count.get("SITELINK", 0),
                "callout":            type_count.get("CALLOUT", 0),
                "structured_snippet": type_count.get("STRUCTURED_SNIPPET", 0),
                "call":               type_count.get("CALL", 0),
                "image":              type_count.get("IMAGE", 0),
            },
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"[JSON] {path}")
    return path


def save_csv(rows: list, site_id: str, ts: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{site_id}_assets_{ts}.csv"

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"[CSV] {path}")
    return path

# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads 広告アセット取得")
    parser.add_argument("--site",      required=True,  help="サイトID（例: 065）")
    parser.add_argument("--campaign",  default="",     help="キャンペーンID（省略時は全件）")
    parser.add_argument("--types",     default="",     help="取得タイプ（カンマ区切り: sitelink,callout,structured_snippet,call,image）")
    parser.add_argument("--no-account-level", action="store_true", help="アカウントレベルのアセットを除外")
    args = parser.parse_args()

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

    # ── 取得タイプの決定 ──
    if args.types:
        types_to_fetch = [t.strip().lower() for t in args.types.split(",")]
    else:
        types_to_fetch = ASSET_TYPES_ALL

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"[INFO] サイト: {args.site} / 顧客ID: {cid}")
    if campaign_id:
        print(f"[INFO] キャンペーンフィルタ: {campaign_id}")
    print(f"[INFO] 取得タイプ: {', '.join(types_to_fetch)}")

    all_rows = []

    # ── キャンペーンレベル取得 ──
    if "sitelink" in types_to_fetch:
        print("[INFO] サイトリンク（campaign_asset）を取得中...")
        rows = fetch_sitelinks(cid, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "callout" in types_to_fetch:
        print("[INFO] コールアウト（campaign_asset）を取得中...")
        rows = fetch_callouts(cid, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "structured_snippet" in types_to_fetch:
        print("[INFO] 構造化スニペット（campaign_asset）を取得中...")
        rows = fetch_structured_snippets(cid, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "call" in types_to_fetch:
        print("[INFO] 電話番号アセット（campaign_asset）を取得中...")
        rows = fetch_call_assets(cid, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    if "image" in types_to_fetch:
        print("[INFO] 画像アセット（campaign_asset）を取得中...")
        rows = fetch_image_assets(cid, campaign_filter, creds, token)
        print(f"  → {len(rows)} 件")
        all_rows.extend(rows)

    # ── アカウントレベル取得 ──
    if not args.no_account_level and not campaign_id:
        print("[INFO] アカウントレベルのアセットを取得中...")

        if "sitelink" in types_to_fetch:
            rows = fetch_account_sitelinks(cid, creds, token)
            print(f"  サイトリンク（ACCOUNT）: {len(rows)} 件")
            all_rows.extend(rows)

        if "callout" in types_to_fetch:
            rows = fetch_account_callouts(cid, creds, token)
            print(f"  コールアウト（ACCOUNT）: {len(rows)} 件")
            all_rows.extend(rows)

        if "call" in types_to_fetch:
            rows = fetch_account_call(cid, creds, token)
            print(f"  電話番号（ACCOUNT）: {len(rows)} 件")
            all_rows.extend(rows)

    print(f"[INFO] 合計: {len(all_rows)} 件")

    # ── 出力 ──
    if all_rows:
        save_json(all_rows, args.site, ts)
        save_csv(all_rows, args.site, ts)
    else:
        print("[WARN] 取得データが0件でした")

    print("[INFO] 完了")


if __name__ == "__main__":
    main()
