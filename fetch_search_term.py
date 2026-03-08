#!/usr/bin/env python3
"""
Google Ads 検索語句別・日別データ取得スクリプト
管理画面「検索語句別・日別」CSVと同じ列構成で出力する

【使い方】
  python3 fetch_search_term.py --site 065 --from 2026-01-01 --to 2026-01-31

  # 特定キャンペーンのみ
  python3 fetch_search_term.py --site 065 --from 2026-01-01 --to 2026-01-31 --campaign 23335569301

  # 管理画面CSVと照合
  python3 fetch_search_term.py --site 065 --from 2026-01-01 --to 2026-01-31 \
      --csv /path/to/065検索語句別・日別.csv

【出力列（管理画面CSV互換）】
  日, 検索語句, キーワード ID, キャンペーン, 広告グループ,
  検索キーワードのマッチタイプ, 検索キーワード,
  表示回数, クリック数, 通貨コード, 費用,
  コンバージョン, すべてのコンバージョン,
  コンバージョン率, コンバージョン値, コンバージョン単価,
  平均クリック単価, クリック率

【API制限メモ】
  - search_term_view はプライバシー保護のため、閾値未満の検索語句は "その他の検索語句" に集約
  - 集約行は ad_group_criterion が存在しないため keyword_id・keyword_text は空
"""

import json
import sys
import csv
import io
import argparse
import requests
from datetime import datetime
from math import isnan
from pathlib import Path

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
    "検索語句",
    "キーワード ID",
    "キャンペーン",
    "広告グループ",
    "検索キーワードのマッチタイプ",
    "検索キーワード",
    "表示回数",
    "クリック数",
    "通貨コード",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "コンバージョン率",
    "コンバージョン値",
    "コンバージョン単価",
    "平均クリック単価",
    "クリック率",
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
# 検索語句データ取得
# ============================================================

def fetch_search_term_data(creds: dict, token: str, customer_id: str,
                           date_from: str, date_to: str,
                           campaign_id: str = None) -> list:
    """
    search_term_view から日別検索語句データを取得する。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            segments.date,
            search_term_view.search_term,
            ad_group_criterion.criterion_id,
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.keyword.text,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions,
            metrics.conversions_from_interactions_rate,
            metrics.conversions_value,
            metrics.cost_per_conversion,
            metrics.average_cpc,
            metrics.ctr
        FROM search_term_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
          {campaign_filter}
        ORDER BY segments.date, campaign.name, ad_group.name,
                 search_term_view.search_term
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# 値フォーマット
# ============================================================

def fmt_pct(val, decimal: int = 2) -> str:
    """0〜1 のレートを XX.XX% 形式に変換。null / NaN は '0.00%'"""
    if val is None:
        return f"0.{'0' * decimal}%"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return f"0.{'0' * decimal}%"
    if isnan(f):
        return f"0.{'0' * decimal}%"
    return f"{f * 100:.{decimal}f}%"


def fmt_yen_int(micros) -> str:
    """マイクロ円 → 整数円の文字列。0 は '0'"""
    if micros is None:
        return "0"
    try:
        f = float(micros)
    except (ValueError, TypeError):
        return "0"
    if isnan(f):
        return "0"
    val = round(f / 1_000_000)
    return str(val)


def fmt_cv_value(val) -> str:
    """コンバージョン値: 'XX,XXX.XX' 形式。0 は '0.00'"""
    if val is None:
        return "0.00"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return "0.00"
    if isnan(f):
        return "0.00"
    if f == 0:
        return "0.00"
    return f"{f:,.2f}"


# ============================================================
# 1行変換
# ============================================================

def row_to_csv_format(r: dict) -> dict:
    """APIレスポンス1行を管理画面CSV列のdictに変換する"""
    seg   = r.get("segments", {})
    stv   = r.get("searchTermView", {})
    crit  = r.get("adGroupCriterion", {})
    kw    = crit.get("keyword", {})
    cmp   = r.get("campaign", {})
    ag    = r.get("adGroup", {})
    m     = r.get("metrics", {})

    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions",    0))
    allcv       = float(m.get("allConversions", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))

    match_type_raw = kw.get("matchType", "")
    match_type_ja  = MATCH_TYPE_JA.get(match_type_raw, match_type_raw)

    # 管理画面では 0クリック行のCVRは "0"、クリックあり0CV行は "0.00%"
    cvr_raw = m.get("conversionsFromInteractionsRate")
    if clk == 0:
        cvr_str = "0"
    else:
        cvr_str = fmt_pct(cvr_raw)

    return {
        "日":                         seg.get("date", ""),
        "検索語句":                    stv.get("searchTerm", ""),
        "キーワード ID":               crit.get("criterionId", ""),
        "キャンペーン":                 cmp.get("name", ""),
        "広告グループ":                 ag.get("name", ""),
        "検索キーワードのマッチタイプ":  match_type_ja,
        "検索キーワード":               kw.get("text", ""),
        "表示回数":                     imp,
        "クリック数":                   clk,
        "通貨コード":                   "JPY",
        "費用":                         str(round(cost_yen)),
        "コンバージョン":               f"{conv:.2f}",
        "すべてのコンバージョン":        f"{allcv:.2f}",
        "コンバージョン率":             cvr_str,
        "コンバージョン値":             fmt_cv_value(m.get("conversionsValue")),
        "コンバージョン単価":           fmt_yen_int(m.get("costPerConversion")),
        "平均クリック単価":             fmt_yen_int(m.get("averageCpc")),
        "クリック率":                   fmt_pct(m.get("ctr")),
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
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("検索語句別・日別\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ CSV保存: {out_path}  ({len(csv_rows):,} 行)")


# ============================================================
# 管理画面CSVとの照合
# ============================================================

def load_csv_totals(path: str) -> dict:
    with open(path, encoding="utf-8-sig") as f:
        lines = f.readlines()
    content = "".join(lines[2:])

    def safe(v):
        v = str(v).strip().replace(",", "").replace("%", "")
        if v in ("--", " --", ""):
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
    parser = argparse.ArgumentParser(description="Google Ads 検索語句別・日別データ取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",     dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",       dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign", default=None,     help="特定キャンペーンIDで絞り込み")
    parser.add_argument("--csv",      default=None,     help="管理画面CSVパス（照合用）")
    args = parser.parse_args()

    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]

    print(f"アカウント : {account['name']} ({cid})")
    print(f"期間       : {args.date_from} 〜 {args.date_to}")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")

    token = get_access_token(creds)

    print("\n検索語句データ取得中...")
    rows = fetch_search_term_data(creds, token, cid,
                                  args.date_from, args.date_to, args.campaign)
    print(f"  取得行数: {len(rows):,} 件")

    csv_rows = [row_to_csv_format(r) for r in rows]

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

    # JSON保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_out = OUTPUT_DIR / f"{account['site_id']}_search_term_{args.date_from}_{args.date_to}.json"
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

    # CSV保存（常時）
    csv_out = OUTPUT_DIR / f"{account['site_id']}_search_term_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, account["name"], args.date_from, args.date_to)

    # 管理画面CSVとの照合
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        mc = load_csv_totals(args.csv)
        print(f"  管理画面行数: {mc['rows']:,}  API行数: {api['rows']:,}")
        print(f"  ※ APIはプライバシー閾値未満の検索語句を集約するため行数は少ない場合あり")
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
            tol = max(1.0, mc["rows"] * 0.5) if "費用" in label else 0.01
            ok  = "✓" if abs(diff) <= tol else "✗"
            if ok == "✗":
                all_ok = False
            print(f"  {label:<25} {cv:>14,.2f} {av:>14,.2f} {diff:>10,.2f}  {ok}")

        print()
        if all_ok:
            print("  ✓ 全指標が一致しました（許容誤差内）")
        else:
            print("  ✗ 一致しない指標があります")
            print("  ヒント: APIのプライバシー集約により一部の語句が '(その他)' に集約されている可能性があります")


if __name__ == "__main__":
    main()
