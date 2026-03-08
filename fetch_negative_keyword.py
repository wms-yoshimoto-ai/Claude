#!/usr/bin/env python3
"""
Google Ads 除外キーワード取得スクリプト
管理画面「除外キーワードのレポート」CSVと同じ列構成で出力する

【使い方】
  python3 fetch_negative_keyword.py --site 065

  # 特定キャンペーンのみ
  python3 fetch_negative_keyword.py --site 065 --campaign 23335615195

  # 管理画面CSVと照合
  python3 fetch_negative_keyword.py --site 065 \
      --csv /path/to/065除外キーワードのレポート.csv

【出力列（管理画面CSV互換）】
  除外キーワード, キーワードまたはリスト, キャンペーン, 広告グループ, レベル, マッチタイプ

【取得内容】
  1. 広告グループレベル除外キーワード (ad_group_criterion, negative=TRUE)
  2. キャンペーンレベル除外キーワード (campaign_criterion, negative=TRUE)
  3. 除外キーワードリスト (campaign_shared_set + shared_set)

【制約・注意事項】
  - 除外キーワードリストの場合、マッチタイプは " --"（管理画面と同じ）
  - 除外キーワードリスト内の個別キーワードは別途 shared_criterion で取得可能だが
    管理画面CSV互換の形式ではリスト名のみ表示
  - パフォーマンス指標（imp/clk等）は除外キーワードには存在しない
"""

import json
import sys
import csv
import io
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

# 管理画面CSVと同じ列順
CSV_COLUMNS = [
    "除外キーワード",
    "キーワードまたはリスト",
    "キャンペーン",
    "広告グループ",
    "レベル",
    "マッチタイプ",
]

# マッチタイプの変換マップ（API値 → 管理画面表示）
MATCH_TYPE_MAP = {
    "EXACT":  "完全一致",
    "PHRASE": "フレーズ一致",
    "BROAD":  "部分一致",
}

# ============================================================
# 認証・アカウント情報の読み込み
# ============================================================

def load_credentials():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)

def load_account(site_id: str) -> dict:
    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)["accounts"]
    acct = next((a for a in accounts if a.get("site_id") == site_id), None)
    if not acct:
        raise ValueError(f"サイトID '{site_id}' がaccounts.jsonに見つかりません")
    return acct

def get_access_token(creds: dict) -> str:
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": creds["refresh_token"],
            "grant_type":    "refresh_token",
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def gaql_request(customer_id: str, gaql: str, creds: dict, token: str) -> list:
    """GAQL クエリを実行してレスポンスのリストを返す"""
    url = f"https://googleads.googleapis.com/v22/customers/{customer_id}/googleAds:searchStream"
    headers = {
        "Authorization":    f"Bearer {token}",
        "developer-token":  creds["developer_token"],
        "login-customer-id": creds.get("manager_customer_id", ""),
        "Content-Type":     "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": gaql})
    if resp.status_code != 200:
        print(f"[ERROR] GAQL失敗: {resp.status_code}", file=sys.stderr)
        print(resp.text, file=sys.stderr)
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
# データ取得関数
# ============================================================

def fetch_ad_group_negative_keywords(customer_id: str, campaign_filter: str,
                                      creds: dict, token: str) -> list:
    """広告グループレベルの除外キーワードを取得"""
    gaql = f"""
        SELECT
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.negative,
            ad_group.name,
            campaign.name
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = 'KEYWORD'
          AND ad_group_criterion.negative = TRUE
          AND ad_group_criterion.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, ad_group.name, ad_group_criterion.keyword.text
    """
    return gaql_request(customer_id, gaql, creds, token)

def fetch_campaign_negative_keywords(customer_id: str, campaign_filter: str,
                                      creds: dict, token: str) -> list:
    """キャンペーンレベルの除外キーワードを取得"""
    gaql = f"""
        SELECT
            campaign_criterion.keyword.text,
            campaign_criterion.keyword.match_type,
            campaign_criterion.negative,
            campaign.name
        FROM campaign_criterion
        WHERE campaign_criterion.type = 'KEYWORD'
          AND campaign_criterion.negative = TRUE
          {campaign_filter}
        ORDER BY campaign.name, campaign_criterion.keyword.text
    """
    return gaql_request(customer_id, gaql, creds, token)

def fetch_negative_keyword_lists(customer_id: str, campaign_filter: str,
                                  creds: dict, token: str) -> list:
    """キャンペーンに適用されている除外キーワードリストを取得"""
    gaql = f"""
        SELECT
            campaign_shared_set.campaign,
            campaign_shared_set.shared_set,
            shared_set.name,
            shared_set.type,
            campaign.name
        FROM campaign_shared_set
        WHERE shared_set.type = 'NEGATIVE_KEYWORDS'
          AND campaign_shared_set.status = 'ENABLED'
          {campaign_filter}
        ORDER BY campaign.name, shared_set.name
    """
    return gaql_request(customer_id, gaql, creds, token)

# ============================================================
# CSV 行生成
# ============================================================

def build_rows(ad_group_results: list, campaign_results: list,
               list_results: list) -> list:
    """取得結果を管理画面互換のCSV行リストに変換"""
    rows = []

    # 1. 除外キーワードリスト（キャンペーンレベル）
    for r in list_results:
        ss = r.get("sharedSet", {})
        cpn = r.get("campaign", {})
        rows.append({
            "除外キーワード":     ss.get("name", ""),
            "キーワードまたはリスト": "リスト",
            "キャンペーン":       cpn.get("name", ""),
            "広告グループ":       " --",
            "レベル":            "キャンペーン",
            "マッチタイプ":       " --",
        })

    # 2. キャンペーンレベル除外キーワード（個別）
    for r in campaign_results:
        cc  = r.get("campaignCriterion", {})
        kwd = cc.get("keyword", {})
        cpn = r.get("campaign", {})
        match_type_raw = kwd.get("matchType", "")
        rows.append({
            "除外キーワード":     kwd.get("text", ""),
            "キーワードまたはリスト": "キーワード",
            "キャンペーン":       cpn.get("name", ""),
            "広告グループ":       " --",
            "レベル":            "キャンペーン",
            "マッチタイプ":       MATCH_TYPE_MAP.get(match_type_raw, match_type_raw),
        })

    # 3. 広告グループレベル除外キーワード
    for r in ad_group_results:
        agc = r.get("adGroupCriterion", {})
        kwd = agc.get("keyword", {})
        ag  = r.get("adGroup", {})
        cpn = r.get("campaign", {})
        match_type_raw = kwd.get("matchType", "")
        rows.append({
            "除外キーワード":     kwd.get("text", ""),
            "キーワードまたはリスト": "キーワード",
            "キャンペーン":       cpn.get("name", ""),
            "広告グループ":       ag.get("name", ""),
            "レベル":            "広告グループ",
            "マッチタイプ":       MATCH_TYPE_MAP.get(match_type_raw, match_type_raw),
        })

    return rows

# ============================================================
# 照合
# ============================================================

def compare_with_csv(rows: list, csv_path: str):
    """管理画面CSVと照合して差分を出力"""
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        mgmt_rows = list(reader)

    # ヘッダー行スキップ（「全期間」などの行）
    mgmt_rows = [r for r in mgmt_rows if r.get("除外キーワード") and
                 r.get("除外キーワード") not in ("除外キーワード",)]

    # キーを(除外キーワード, キャンペーン, 広告グループ, レベル)で比較
    def row_key(r):
        return (
            r.get("除外キーワード", "").strip('"'),
            r.get("キャンペーン", "").strip(),
            r.get("広告グループ", "").strip(),
            r.get("レベル", "").strip(),
        )

    api_keys  = {row_key(r) for r in rows}
    mgmt_keys = {row_key(r) for r in mgmt_rows}

    only_mgmt = mgmt_keys - api_keys
    only_api  = api_keys  - mgmt_keys

    print(f"\n照合結果:")
    print(f"  管理画面CSV: {len(mgmt_rows)}件")
    print(f"  API取得:     {len(rows)}件")

    if only_mgmt:
        print(f"\n管理画面にのみ存在 ({len(only_mgmt)}件):")
        for k in sorted(only_mgmt):
            print(f"  {k}")
    if only_api:
        print(f"\nAPIにのみ存在 ({len(only_api)}件):")
        for k in sorted(only_api):
            print(f"  {k}")
    if not only_mgmt and not only_api:
        print("  ✓ 完全一致")

# ============================================================
# JSON 出力
# ============================================================

def save_json(rows: list, site_id: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"{site_id}_negative_keyword_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "site_id":      site_id,
            "fetched_at":   datetime.now().isoformat(),
            "total_rows":   len(rows),
            "rows":         rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"[JSON] {path}")
    return path

# ============================================================
# CSV 出力
# ============================================================

def save_csv(rows: list, site_id: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"{site_id}_negative_keyword_{ts}.csv"

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        # 管理画面と同じヘッダー行を再現
        f.write("除外キーワードのレポート\n")
        f.write("全期間\n")
        writer.writeheader()
        writer.writerows(rows)

    print(f"[CSV] {path}")
    return path

# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads 除外キーワード取得")
    parser.add_argument("--site",     required=True, help="サイトID（例: 065）")
    parser.add_argument("--campaign", default="",    help="キャンペーンID（省略時は全件）")
    parser.add_argument("--csv",      default="",    help="照合する管理画面CSVのパス")
    args = parser.parse_args()

    # ── アカウント情報の読み込み ──
    creds  = load_credentials()
    acct   = load_account(args.site)
    cid    = acct["customer_id"].replace("-", "")
    token  = get_access_token(creds)

    # ── キャンペーンIDの解決 ──
    campaign_id = ""
    if args.campaign:
        campaign_id = resolve_campaign_id(args.campaign, args.site)

    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""

    print(f"[INFO] サイト: {args.site} / 顧客ID: {cid}")
    if campaign_id:
        print(f"[INFO] キャンペーンフィルタ: {campaign_id}")
    else:
        print("[INFO] 全キャンペーンを対象")

    # ── データ取得 ──
    print("[INFO] 除外キーワードリスト（campaign_shared_set）を取得中...")
    list_results = fetch_negative_keyword_lists(cid, campaign_filter, creds, token)
    print(f"  → {len(list_results)} 件")

    print("[INFO] キャンペーンレベル除外キーワード（campaign_criterion）を取得中...")
    campaign_neg_results = fetch_campaign_negative_keywords(cid, campaign_filter, creds, token)
    print(f"  → {len(campaign_neg_results)} 件")

    print("[INFO] 広告グループレベル除外キーワード（ad_group_criterion）を取得中...")
    ag_neg_results = fetch_ad_group_negative_keywords(cid, campaign_filter, creds, token)
    print(f"  → {len(ag_neg_results)} 件")

    # ── CSV行生成 ──
    rows = build_rows(ag_neg_results, campaign_neg_results, list_results)
    print(f"[INFO] 合計: {len(rows)} 件")

    # ── 出力 ──
    save_json(rows, args.site)
    save_csv(rows, args.site)

    # ── 照合（オプション）──
    if args.csv:
        compare_with_csv(rows, args.csv)

    print("[INFO] 完了")

if __name__ == "__main__":
    main()
