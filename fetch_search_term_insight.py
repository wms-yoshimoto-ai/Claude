#!/usr/bin/env python3
"""
Google Ads Pmax 検索語句インサイト取得スクリプト
campaign_search_term_insight リソースからPmaxキャンペーンの検索カテゴリ別パフォーマンスを取得する

【背景】
  search_term_view はPmaxでは個別検索語句を返さない（0件）。
  代わりに campaign_search_term_insight で「検索カテゴリ」単位のデータが取得可能。
  管理画面「分析情報 > 検索語句」と同等レベルのカテゴリデータ。

【使い方】
  python3 fetch_search_term_insight.py --site 072 --from 2026-02-01 --to 2026-02-28

  # 特定キャンペーンのみ
  python3 fetch_search_term_insight.py --site 072 --from 2026-02-01 --to 2026-02-28 --campaign 23367869010

【出力列】
  キャンペーン, キャンペーン ID, 検索カテゴリ, カテゴリID,
  表示回数, クリック数, コンバージョン, クリック率, コンバージョン率

【備考】
  - category_label が空文字のカテゴリ＝未分類（API側でラベルなし）
  - Pmaxキャンペーン専用（検索キャンペーンは search_term_view を使用）
  - 日別分割は segments.date で対応可能だが、デフォルトは期間集計
  - campaign_search_term_insight は cost_micros / all_conversions / conversions_value 非対応
    （PROHIBITED_METRIC）。費用・CPA等の算出は不可
"""

import json
import sys
import csv
import argparse
import requests
from datetime import datetime
from math import isnan
from pathlib import Path
from campaign_db import resolve_campaign_id

# ============================================================
# パス設定
# ============================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"

CSV_COLUMNS = [
    "キャンペーン",
    "キャンペーン ID",
    "検索カテゴリ",
    "カテゴリID",
    "表示回数",
    "クリック数",
    "コンバージョン",
    "クリック率",
    "コンバージョン率",
]

CSV_DAILY_COLUMNS = ["日"] + CSV_COLUMNS


# ============================================================
# 設定ファイルの読み込み
# ============================================================

def load_credentials():
    if not CREDENTIALS_FILE.exists():
        print(f"エラー: {CREDENTIALS_FILE} が見つかりません")
        sys.exit(1)
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)["google_ads"]


def load_account(site_query: str) -> dict:
    if not ACCOUNTS_FILE.exists():
        print(f"エラー: {ACCOUNTS_FILE} が見つかりません")
        sys.exit(1)
    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)["accounts"]
    q = site_query.strip()
    q_no_hyphen = q.replace("-", "")
    matched = [
        a for a in accounts
        if a.get("site_id") == q
        or a.get("name") == q
        or a.get("customer_id") == q
        or a["customer_id"].replace("-", "") == q_no_hyphen
    ]
    if not matched:
        print(f"エラー: '{site_query}' に一致するアカウントが見つかりません")
        sys.exit(1)
    return matched[0]


# ============================================================
# 認証
# ============================================================

def get_access_token(creds: dict) -> str:
    oauth = creds["oauth"]
    res = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     oauth["client_id"],
        "client_secret": oauth["client_secret"],
        "refresh_token": oauth["refresh_token"],
        "grant_type":    "refresh_token",
    }, timeout=30)
    if res.status_code != 200:
        raise Exception(f"トークン取得エラー: {res.text[:200]}")
    return res.json()["access_token"]


# ============================================================
# Google Ads API 検索
# ============================================================

def search_all(creds: dict, token: str, customer_id: str, gaql: str) -> list:
    cid = customer_id.replace("-", "").replace(" ", "")
    url = f"https://googleads.googleapis.com/v22/customers/{cid}/googleAds:search"
    headers = {
        "Authorization":     f"Bearer {token}",
        "developer-token":   creds["developer_token"],
        "login-customer-id": creds["mcc_customer_id"],
        "Content-Type":      "application/json",
    }
    results = []
    page_token = None
    while True:
        payload = {"query": gaql}
        if page_token:
            payload["pageToken"] = page_token
        res = requests.post(url, headers=headers, json=payload, timeout=120)
        if res.status_code != 200:
            raise Exception(f"API Error [{res.status_code}]: {res.text[:500]}")
        data = res.json()
        results.extend(data.get("results", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results


# ============================================================
# 検索語句インサイト取得
# ============================================================

def fetch_search_term_insight(creds: dict, token: str, customer_id: str,
                               date_from: str, date_to: str,
                               campaign_id: str = None,
                               daily: bool = False) -> list:
    """
    campaign_search_term_insight からPmaxの検索カテゴリ別データを取得。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    date_select = "segments.date," if daily else ""
    date_order  = "segments.date," if daily else ""

    gaql = f"""
        SELECT
            {date_select}
            campaign.name,
            campaign.id,
            campaign_search_term_insight.category_label,
            campaign_search_term_insight.id,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions
        FROM campaign_search_term_insight
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY {date_order} metrics.impressions DESC
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# 値フォーマット
# ============================================================

def fmt_cv(val) -> str:
    if val is None:
        return "0.00"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return "0.00"
    return f"{f:.2f}"


def fmt_pct(val) -> str:
    if val is None:
        return " --"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return " --"
    if isnan(f):
        return " --"
    return f"{f:.2f}%"


# ============================================================
# 1行変換
# ============================================================

def row_to_csv_format(r: dict, daily: bool = False) -> dict:
    """APIレスポンス1行をCSV列のdictに変換する"""
    cmp     = r.get("campaign", {})
    insight = r.get("campaignSearchTermInsight", {})
    seg     = r.get("segments", {})
    m       = r.get("metrics", {})

    conv        = float(m.get("conversions", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))

    # CTR, CVR（cost系メトリクスはcampaign_search_term_insightで非対応）
    ctr = (clk / imp * 100) if imp > 0 else 0
    cvr = (conv / clk * 100) if clk > 0 else 0

    label = insight.get("categoryLabel", "")
    if not label:
        label = "（未分類）"

    row = {
        "キャンペーン":              cmp.get("name", ""),
        "キャンペーン ID":           str(cmp.get("id", "")),
        "検索カテゴリ":              label,
        "カテゴリID":                insight.get("id", ""),
        "表示回数":                  imp,
        "クリック数":                clk,
        "コンバージョン":            fmt_cv(conv),
        "クリック率":                fmt_pct(ctr),
        "コンバージョン率":          fmt_pct(cvr),
        # 照合用（JSON内部のみ）
        "_conv_exact": conv,
    }

    if daily:
        row["日"] = seg.get("date", "")

    return row


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(csv_rows: list, out_path: Path,
               account_name: str, date_from: str, date_to: str,
               daily: bool = False):
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    columns = CSV_DAILY_COLUMNS if daily else CSV_COLUMNS

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("Pmax 検索語句インサイト（検索カテゴリ別）\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ CSV保存: {out_path}  ({len(csv_rows):,} 行)")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads Pmax 検索語句インサイト取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",     dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",       dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign", default=None,     help="特定キャンペーンIDで絞り込み")
    parser.add_argument("--daily",    action="store_true", help="日別モード")
    args = parser.parse_args()

    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]
    site_id = account["site_id"]

    if args.campaign:
        args.campaign = resolve_campaign_id(site_id, args.campaign)

    print(f"アカウント : {account['name']} ({cid})")
    print(f"期間       : {args.date_from} 〜 {args.date_to}")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")
    if args.daily:
        print(f"モード     : 日別")

    token = get_access_token(creds)

    print("\n検索語句インサイト取得中...")
    raw_rows = fetch_search_term_insight(creds, token, cid,
                                          args.date_from, args.date_to,
                                          args.campaign, args.daily)
    print(f"  取得行数: {len(raw_rows):,} 件")

    csv_rows = [row_to_csv_format(r, daily=args.daily) for r in raw_rows]

    # 集計
    api_totals = {
        "rows":  len(csv_rows),
        "imp":   sum(r["表示回数"]      for r in csv_rows),
        "clk":   sum(r["クリック数"]    for r in csv_rows),
        "conv":  sum(r["_conv_exact"]  for r in csv_rows),
    }

    print("\n【API取得結果】")
    print(f"  行数             : {api_totals['rows']:,} 件")
    print(f"  表示回数         : {api_totals['imp']:,}")
    print(f"  クリック数       : {api_totals['clk']:,}")
    print(f"  コンバージョン   : {api_totals['conv']:.2f}")

    # カテゴリ別サマリー（期間集計時のみ表示）
    if not args.daily:
        print(f"\n【検索カテゴリ別（imp上位15件）】")
        print(f"  {'検索カテゴリ':<35} {'imp':>8} {'clk':>6} {'conv':>6} {'CVR':>7}")
        print("  " + "-" * 65)
        sorted_rows = sorted(csv_rows, key=lambda r: r["表示回数"], reverse=True)
        for r in sorted_rows[:15]:
            label = r["検索カテゴリ"][:35]
            cvr = r["コンバージョン率"]
            print(f"  {label:<35} {r['表示回数']:>8,} {r['クリック数']:>6,} "
                  f"{r['コンバージョン']:>6} {cvr:>7}")

    # ============================================================
    # JSON保存
    # ============================================================
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    mode_suffix = "_daily" if args.daily else ""
    json_out = OUTPUT_DIR / f"{site_id}_search_term_insight{mode_suffix}_{args.date_from}_{args.date_to}.json"

    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "account":     account,
            "period":      {"from": args.date_from, "to": args.date_to},
            "campaign_id": args.campaign,
            "mode":        "daily" if args.daily else "period",
            "fetched_at":  datetime.now().isoformat(),
            "api_totals":  api_totals,
            "rows":        csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    # CSV保存
    csv_out = OUTPUT_DIR / f"{site_id}_search_term_insight{mode_suffix}_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, account["name"],
               args.date_from, args.date_to, daily=args.daily)


if __name__ == "__main__":
    main()
