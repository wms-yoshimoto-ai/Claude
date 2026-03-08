#!/usr/bin/env python3
"""
Google Ads キーワード別・日別データ取得スクリプト
管理画面「キーワード別・日別」CSVと同じ列構成で出力する

【使い方】
  python3 fetch_keyword.py --site 065 --from 2026-01-01 --to 2026-01-31

  # 特定キャンペーンのみ
  python3 fetch_keyword.py --site 065 --from 2026-01-01 --to 2026-01-31 --campaign 23335569301

  # 管理画面CSVと照合
  python3 fetch_keyword.py --site 065 --from 2026-01-01 --to 2026-01-31 \
      --csv /path/to/065キーワード別・日別.csv

【出力列（管理画面CSV互換）】
  日, キャンペーン, 広告グループ, キーワード ID, 検索キーワード, 検索キーワードのマッチタイプ,
  表示回数, クリック数, 通貨コード, 費用, コンバージョン, すべてのコンバージョン,
  検索広告の完全一致の IS, 検索広告のインプレッション シェア, 検索広告の IS 損失率（ランク）,
  最上部インプレッションの割合, 上部インプレッションの割合, クリックシェア
"""

import json
import sys
import csv
import io
import argparse
import requests
from datetime import datetime
from pathlib import Path
from math import isnan
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
    "日",
    "キャンペーン",
    "広告グループ",
    "キーワード ID",
    "検索キーワード",
    "検索キーワードのマッチタイプ",
    "表示回数",
    "クリック数",
    "通貨コード",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "検索広告の完全一致の IS",
    "検索広告のインプレッション シェア",
    "検索広告の IS 損失率（ランク）",
    "最上部インプレッションの割合",
    "上部インプレッションの割合",
    "クリックシェア",
]

MATCH_TYPE_JA = {
    "EXACT":   "完全一致",
    "PHRASE":  "フレーズ一致",
    "BROAD":   "インテントマッチ",
}


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
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        if res.status_code != 200:
            raise Exception(f"API Error [{res.status_code}]: {res.text[:500]}")
        data = res.json()
        results.extend(data.get("results", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results


# ============================================================
# キーワードデータ取得
# ============================================================

def fetch_keyword_data(creds: dict, token: str, customer_id: str,
                       date_from: str, date_to: str,
                       campaign_id: str = None) -> list:
    """
    keyword_view から日別キーワードデータを取得する。
    IS系指標・クリックシェアを含む全18列を取得。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            segments.date,
            campaign.name,
            ad_group.name,
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions,
            metrics.search_exact_match_impression_share,
            metrics.search_impression_share,
            metrics.search_rank_lost_impression_share,
            metrics.absolute_top_impression_percentage,
            metrics.top_impression_percentage
        FROM keyword_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
          AND ad_group_criterion.status != 'REMOVED'
          {campaign_filter}
        ORDER BY segments.date, campaign.name, ad_group.name,
                 ad_group_criterion.keyword.text
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# IS値のフォーマット
# ============================================================

def fmt_is(val) -> str:
    """
    impressionShare系の値をフォーマットする。
    API は 0.0〜1.0 の float を返す。null / NaN は " --"。
    """
    if val is None:
        return " --"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return " --"
    if isnan(f):
        return " --"
    return f"{f * 100:.2f}%"


def fmt_pct(val):
    """
    top_impression_percentage / absolute_top_impression_percentage 用。
    管理画面CSV では表示回数が0の行は 0 (数値) で出力される。
    """
    if val is None:
        return 0
    try:
        f = float(val)
    except (ValueError, TypeError):
        return 0
    if isnan(f):
        return 0
    return f"{f * 100:.2f}%"


# ============================================================
# 1行変換
# ============================================================

def row_to_csv_format(r: dict) -> dict:
    """APIレスポンス1行を管理画面CSV列のdictに変換する"""
    seg   = r.get("segments", {})
    cmp   = r.get("campaign", {})
    ag    = r.get("adGroup", {})
    crit  = r.get("adGroupCriterion", {})
    kw    = crit.get("keyword", {})
    m     = r.get("metrics", {})

    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions",    0))
    allcv       = float(m.get("allConversions", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))

    match_type_raw = kw.get("matchType", "")
    match_type_ja  = MATCH_TYPE_JA.get(match_type_raw, match_type_raw)

    # IS 系
    search_exact_is    = m.get("searchExactMatchImpressionShare")
    search_is          = m.get("searchImpressionShare")
    search_is_loss_rnk = m.get("searchRankLostImpressionShare")
    abs_top_imp        = m.get("absoluteTopImpressionPercentage")
    top_imp            = m.get("topImpressionPercentage")
    # click_share は keyword_view では取得不可（管理画面同様 " --" で出力）

    return {
        "日":                              seg.get("date", ""),
        "キャンペーン":                     cmp.get("name", ""),
        "広告グループ":                     ag.get("name", ""),
        "キーワード ID":                    crit.get("criterionId", ""),
        "検索キーワード":                   kw.get("text", ""),
        "検索キーワードのマッチタイプ":      match_type_ja,
        "表示回数":                         imp,
        "クリック数":                       clk,
        "通貨コード":                       "JPY",
        "費用":                             str(round(cost_yen)),
        "コンバージョン":                   f"{conv:.2f}",
        "すべてのコンバージョン":            f"{allcv:.2f}",
        "検索広告の完全一致の IS":           fmt_is(search_exact_is),
        "検索広告のインプレッション シェア": fmt_is(search_is),
        "検索広告の IS 損失率（ランク）":    fmt_is(search_is_loss_rnk),
        "最上部インプレッションの割合":      fmt_pct(abs_top_imp),
        "上部インプレッションの割合":        fmt_pct(top_imp),
        "クリックシェア":                   " --",
        # 照合用（表示しない）
        "_cost_exact": cost_yen,
        "_conv_exact":  conv,
        "_allcv_exact": allcv,
    }


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(csv_rows: list, out_path: Path,
               account_name: str, date_from: str, date_to: str):
    """管理画面CSVと同じ形式でエクスポートする"""
    # 日付を管理画面表示形式に変換（2026-01-01 → 2026年1月1日）
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        # ヘッダー2行（管理画面CSVと同じ構造）
        f.write("キーワード別・日別\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ CSV保存: {out_path}  ({len(csv_rows):,} 行)")


# ============================================================
# 管理画面CSVとの照合
# ============================================================

def load_csv_totals(path: str) -> dict:
    """管理画面CSVを読み込んで合計値を返す"""
    with open(path, encoding="utf-8-sig") as f:
        lines = f.readlines()
    content = "".join(lines[2:])  # 先頭2行（タイトル・期間）をスキップ

    def safe(v):
        v = str(v).strip().replace(",", "").replace("%", "")
        if v in ("--", " --", "< 10", "> 90", ""):
            return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0

    rows = list(csv.DictReader(io.StringIO(content)))
    return {
        "rows":  len(rows),
        "imp":   sum(safe(r.get("表示回数", 0))             for r in rows),
        "clk":   sum(safe(r.get("クリック数", 0))           for r in rows),
        "cost":  sum(safe(r.get("費用", 0))                for r in rows),
        "conv":  sum(safe(r.get("コンバージョン", 0))       for r in rows),
        "allcv": sum(safe(r.get("すべてのコンバージョン", 0)) for r in rows),
    }


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads キーワード別・日別データ取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",     dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",       dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign", default=None,     help="特定キャンペーンIDで絞り込み（省略時は全キャンペーン）")
    parser.add_argument("--csv",      default=None,     help="管理画面CSVパス（照合用）")
    args = parser.parse_args()

    # ── 設定読み込み ──────────────────────────────────────
    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]

    # キャンペーン名 → ID 解決（数字IDのままでも可）
    if args.campaign:
        args.campaign = resolve_campaign_id(account["site_id"], args.campaign)

    print(f"アカウント : {account['name']} ({cid})")
    print(f"期間       : {args.date_from} 〜 {args.date_to}")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")

    # ── 認証 ───────────────────────────────────────────────
    token = get_access_token(creds)

    # ── データ取得 ─────────────────────────────────────────
    print("\nキーワードデータ取得中...")
    rows = fetch_keyword_data(creds, token, cid, args.date_from, args.date_to, args.campaign)
    print(f"  取得行数: {len(rows):,} 件")

    # ── CSV形式に変換 ──────────────────────────────────────
    csv_rows = [row_to_csv_format(r) for r in rows]

    # ── API集計 ────────────────────────────────────────────
    api = {
        "rows":  len(csv_rows),
        "imp":   sum(r["表示回数"]      for r in csv_rows),
        "clk":   sum(r["クリック数"]    for r in csv_rows),
        "cost":  sum(r["_cost_exact"]  for r in csv_rows),
        "conv":  sum(r["_conv_exact"]  for r in csv_rows),
        "allcv": sum(r["_allcv_exact"] for r in csv_rows),
    }

    print("\n【API取得結果】")
    print(f"  行数             : {api['rows']:,} 件")
    print(f"  表示回数         : {api['imp']:,}")
    print(f"  クリック数       : {api['clk']:,}")
    print(f"  費用             : {api['cost']:,.0f} 円")
    print(f"  コンバージョン   : {api['conv']}")
    print(f"  全コンバージョン : {api['allcv']}")

    # ── JSON保存 ───────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_out = OUTPUT_DIR / f"{account['site_id']}_keyword_{args.date_from}_{args.date_to}.json"
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "account":     account,
            "period":      {"from": args.date_from, "to": args.date_to},
            "campaign_id": args.campaign,
            "fetched_at":  datetime.now().isoformat(),
            "api_totals":  api,
            "rows":        csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    # ── CSV保存（常時） ────────────────────────────────────
    csv_out = OUTPUT_DIR / f"{account['site_id']}_keyword_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, account["name"], args.date_from, args.date_to)

    # ── 管理画面CSVとの照合 ────────────────────────────────
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        mc = load_csv_totals(args.csv)
        print(f"  管理画面行数: {mc['rows']:,}  API行数: {api['rows']:,}")
        print(f"  ※APIは enabled キーワードのみ集計のため行数は異なる場合があります")
        print()
        print(f"  {'項目':<25} {'管理画面CSV':>14} {'API生成CSV':>14} {'差異':>10} {'一致':>6}")
        print("  " + "-" * 73)

        checks = [
            ("表示回数",        mc["imp"],   api["imp"]),
            ("クリック数",      mc["clk"],   api["clk"]),
            ("費用(円)",        mc["cost"],  api["cost"]),
            ("コンバージョン",  mc["conv"],  api["conv"]),
            ("全コンバージョン",mc["allcv"], api["allcv"]),
        ]
        all_ok = True
        for label, cv, av in checks:
            diff = av - cv
            # 費用は行ごとの整数丸め累積誤差を許容
            tol = max(1.0, mc["rows"] * 0.5) if "費用" in label else 0.01
            ok  = "✓" if abs(diff) <= tol else "✗"
            if ok == "✗":
                all_ok = False
            print(f"  {label:<25} {cv:>14,.2f} {av:>14,.2f} {diff:>10,.2f}  {ok}")

        print()
        if all_ok:
            print("  ✓ 全指標が一致しました（許容誤差内）")
        else:
            print("  ✗ 一致しない指標があります。ログを確認してください。")


if __name__ == "__main__":
    main()
