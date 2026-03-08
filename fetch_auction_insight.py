#!/usr/bin/env python3
"""
Google Ads オークション分析データ取得スクリプト
管理画面「オークション分析」タブと同じ指標をCSV/JSONで出力する

【使い方】
  python3 fetch_auction_insight.py --site 065 --from 2026-01-01 --to 2026-01-31

  # 特定キャンペーンのみ
  python3 fetch_auction_insight.py --site 065 --from 2026-01-01 --to 2026-01-31 \
      --campaign 23335569301

【出力列】
  日, キャンペーン, ドメイン,
  インプレッションシェア, 重複率, 上位掲載率,
  トップオブページ率, 絶対トップ率, 優位性

【制約・注意事項】
  - 検索キャンペーンのみ対象（Pmax は auction_insight に含まれない）
  - データ量が閾値未満の場合、そのドメインは非表示（管理画面と同じ）
  - 各指標は 0〜1 の float（例: 0.45 = 45%）で返る → CSV出力時に XX.XX% 形式に変換
  - 自社ドメインも競合ドメインと同列で返る（"Your domain" または実ドメイン名）
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

# CSV列（管理画面の「オークション分析」タブに合わせた順序）
CSV_COLUMNS = [
    "日",
    "キャンペーン",
    "ドメイン",
    "インプレッションシェア",
    "重複率",
    "上位掲載率",
    "トップオブページ率",
    "絶対トップ率",
    "優位性",
]

# ============================================================
# 認証・アカウント情報
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
# データ取得
# ============================================================

def fetch_auction_insight(customer_id: str, date_from: str, date_to: str,
                          campaign_filter: str, creds: dict, token: str) -> list:
    """オークション分析データを取得"""
    gaql = f"""
        SELECT
            auction_insight_summary.display_name,
            auction_insight_summary.impression_share,
            auction_insight_summary.overlap_rate,
            auction_insight_summary.position_above_rate,
            auction_insight_summary.top_of_page_rate,
            auction_insight_summary.absolute_top_of_page_rate,
            auction_insight_summary.outranking_share,
            campaign.id,
            campaign.name,
            segments.date
        FROM auction_insight
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY segments.date, campaign.name,
                 auction_insight_summary.impression_share DESC
    """
    return gaql_request(customer_id, gaql, creds, token)

# ============================================================
# 整形
# ============================================================

def _fmt_pct(val) -> str:
    """float（0〜1）をパーセント文字列に変換。None/''/0 は ' --'"""
    if val is None or val == "" or val == 0:
        return " --"
    try:
        f = float(val)
        if f == 0:
            return " --"
        return f"{f * 100:.2f}%"
    except (ValueError, TypeError):
        return " --"

def build_rows(results: list) -> list:
    rows = []
    for r in results:
        ai  = r.get("auctionInsightSummary", {})
        cpn = r.get("campaign", {})
        seg = r.get("segments", {})
        rows.append({
            "日":                 seg.get("date", ""),
            "キャンペーン":        cpn.get("name", ""),
            "ドメイン":            ai.get("displayName", ""),
            "インプレッションシェア": _fmt_pct(ai.get("impressionShare")),
            "重複率":              _fmt_pct(ai.get("overlapRate")),
            "上位掲載率":          _fmt_pct(ai.get("positionAboveRate")),
            "トップオブページ率":    _fmt_pct(ai.get("topOfPageRate")),
            "絶対トップ率":        _fmt_pct(ai.get("absoluteTopOfPageRate")),
            "優位性":              _fmt_pct(ai.get("outrankingShare")),
        })
    return rows

# ============================================================
# 出力
# ============================================================

def save_json(results: list, rows: list, site_id: str,
              date_from: str, date_to: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{site_id}_auction_insight_{date_from}_{date_to}.json"

    # ドメイン別サマリー（期間平均）を計算
    from collections import defaultdict
    domain_stats = defaultdict(lambda: {
        "imp_share": [], "overlap": [], "pos_above": [],
        "top_page": [], "abs_top": [], "outranking": []
    })
    for r in results:
        ai  = r.get("auctionInsightSummary", {})
        dom = ai.get("displayName", "")
        def _f(k): return float(ai.get(k) or 0)
        domain_stats[dom]["imp_share"].append(_f("impressionShare"))
        domain_stats[dom]["overlap"].append(_f("overlapRate"))
        domain_stats[dom]["pos_above"].append(_f("positionAboveRate"))
        domain_stats[dom]["top_page"].append(_f("topOfPageRate"))
        domain_stats[dom]["abs_top"].append(_f("absoluteTopOfPageRate"))
        domain_stats[dom]["outranking"].append(_f("outrankingShare"))

    def _avg(lst): return round(sum(lst) / len(lst), 4) if lst else 0

    summary = sorted([
        {
            "domain":          dom,
            "avg_imp_share":   _avg(v["imp_share"]),
            "avg_overlap":     _avg(v["overlap"]),
            "avg_pos_above":   _avg(v["pos_above"]),
            "avg_top_page":    _avg(v["top_page"]),
            "avg_abs_top":     _avg(v["abs_top"]),
            "avg_outranking":  _avg(v["outranking"]),
        }
        for dom, v in domain_stats.items()
    ], key=lambda x: x["avg_imp_share"], reverse=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "site_id":    site_id,
            "period":     {"from": date_from, "to": date_to},
            "fetched_at": datetime.now().isoformat(),
            "total_rows": len(rows),
            "domain_summary": summary,
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)

    print(f"[JSON] {path}")
    return path

def save_csv(rows: list, site_id: str, date_from: str, date_to: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{site_id}_auction_insight_{date_from}_{date_to}.csv"

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[CSV] {path}")
    return path

# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads オークション分析取得")
    parser.add_argument("--site",     required=True, help="サイトID（例: 065）")
    parser.add_argument("--from",     dest="date_from", required=True,
                        help="開始日 YYYY-MM-DD")
    parser.add_argument("--to",       dest="date_to",   required=True,
                        help="終了日 YYYY-MM-DD")
    parser.add_argument("--campaign", default="",
                        help="キャンペーンID（省略時は全検索キャンペーン）")
    args = parser.parse_args()

    creds  = load_credentials()
    acct   = load_account(args.site)
    cid    = acct["customer_id"].replace("-", "")
    token  = get_access_token(creds)

    campaign_id = ""
    if args.campaign:
        campaign_id = resolve_campaign_id(args.campaign, args.site)
    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""

    print(f"[INFO] サイト: {args.site} / 顧客ID: {cid}")
    print(f"[INFO] 期間: {args.date_from} 〜 {args.date_to}")
    if campaign_id:
        print(f"[INFO] キャンペーン: {campaign_id}")

    print("[INFO] オークション分析データを取得中...")
    results = fetch_auction_insight(
        cid, args.date_from, args.date_to, campaign_filter, creds, token)
    print(f"  → {len(results)} 行")

    if not results:
        print("[WARN] データが取得できませんでした。")
        print("       ・データ量が閾値未満の場合、APIは空を返します")
        print("       ・対象が検索キャンペーンか確認してください")
        sys.exit(0)

    rows = build_rows(results)
    json_path = save_json(results, rows, args.site, args.date_from, args.date_to)
    save_csv(rows, args.site, args.date_from, args.date_to)

    # ドメイン別サマリーをコンソール表示
    with open(json_path) as f:
        jdata = json.load(f)
    print(f"\n[INFO] ドメイン別 期間平均インプレッションシェア（降順）:")
    for d in jdata["domain_summary"]:
        print(f"  {d['domain']:<40} IS={d['avg_imp_share']*100:.1f}%"
              f"  重複={d['avg_overlap']*100:.1f}%"
              f"  優位性={d['avg_outranking']*100:.1f}%")

    print("[INFO] 完了")

if __name__ == "__main__":
    main()
