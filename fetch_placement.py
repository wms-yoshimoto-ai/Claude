#!/usr/bin/env python3
"""
Google Ads プレースメントレポート取得スクリプト
Pmax等のキャンペーンがどのネットワーク（検索/ディスプレイ/YouTube等）に配信しているかを取得する

【使い方】
  # ネットワーク別サマリー + 個別プレースメント
  python3 fetch_placement.py --site 072 --from 2026-02-01 --to 2026-02-28

  # 特定キャンペーンのみ
  python3 fetch_placement.py --site 072 --from 2026-02-01 --to 2026-02-28 --campaign 23367869010

  # ネットワークサマリーのみ（プレースメント詳細なし）
  python3 fetch_placement.py --site 072 --from 2026-02-01 --to 2026-02-28 --summary-only

【出力】
  クエリ1: campaign × segments.ad_network_type（ネットワーク別サマリー）
  クエリ2: group_placement_view（個別プレースメント詳細）
  出力ファイル: {site_id}_placement_{from}_{to}.json / .csv
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

# ネットワーク種別の日本語マッピング（管理画面表示名）
NETWORK_TYPE_MAP = {
    "CONTENT":          "ディスプレイ ネットワーク",
    "MIXED":            "クロスネットワーク",
    "SEARCH":           "Google 検索",
    "SEARCH_PARTNERS":  "検索パートナー",
    "UNSPECIFIED":      "未指定",
    "UNKNOWN":          "不明",
    "YOUTUBE_SEARCH":   "YouTube 検索",
    "YOUTUBE_WATCH":    "YouTube 動画",
}

# プレースメントタイプの日本語マッピング
PLACEMENT_TYPE_MAP = {
    "WEBSITE":                  "ウェブサイト",
    "YOUTUBE_CHANNEL":          "YouTube チャンネル",
    "YOUTUBE_VIDEO":            "YouTube 動画",
    "MOBILE_APPLICATION":       "モバイルアプリ",
    "MOBILE_APP_CATEGORY":      "モバイルアプリカテゴリ",
    "GOOGLE_PRODUCTS":          "Google プロダクト",
}

# ネットワークサマリーCSV列
NETWORK_CSV_COLUMNS = [
    "キャンペーン",
    "キャンペーン ID",
    "ネットワーク",
    "ネットワーク（原文）",
    "表示回数",
    "クリック数",
    "通貨コード",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "コンバージョン値",
    "imp割合",
    "cost割合",
]

# プレースメント詳細CSV列
PLACEMENT_CSV_COLUMNS = [
    "キャンペーン",
    "キャンペーン ID",
    "プレースメント",
    "プレースメントの種類",
    "プレースメントURL",
    "表示回数",
    "クリック数",
    "通貨コード",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "コンバージョン値",
    "クリック率",
    "コンバージョン率",
    "平均クリック単価",
]


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
# クエリ1: ネットワーク別サマリー
# ============================================================

def fetch_network_summary(creds: dict, token: str, customer_id: str,
                          date_from: str, date_to: str,
                          campaign_id: str = None) -> list:
    """
    campaign × segments.ad_network_type でネットワーク別のパフォーマンスを取得。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            campaign.name,
            campaign.id,
            campaign.advertising_channel_type,
            segments.ad_network_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, segments.ad_network_type
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# クエリ2: 個別プレースメント詳細
# ============================================================

def fetch_placement_detail(creds: dict, token: str, customer_id: str,
                           date_from: str, date_to: str,
                           campaign_id: str = None) -> list:
    """
    group_placement_view から個別プレースメント（URL/アプリ/YouTubeチャンネル等）を取得。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            campaign.name,
            campaign.id,
            group_placement_view.display_name,
            group_placement_view.placement,
            group_placement_view.placement_type,
            group_placement_view.target_url,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions,
            metrics.conversions_value
        FROM group_placement_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY metrics.impressions DESC
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# 値フォーマット
# ============================================================

def fmt_yen_int(micros) -> str:
    """マイクロ円 → 整数円の文字列"""
    if micros is None:
        return "0"
    try:
        f = float(micros)
    except (ValueError, TypeError):
        return "0"
    if isnan(f):
        return "0"
    return str(round(f / 1_000_000))


def fmt_cv(val) -> str:
    """コンバージョン値を小数2桁文字列に"""
    if val is None:
        return "0.00"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return "0.00"
    return f"{f:.2f}"


def fmt_cv_value(val) -> str:
    """コンバージョン値: 'XX,XXX.XX' 形式"""
    if val is None:
        return "0.00"
    try:
        f = float(val)
    except (ValueError, TypeError):
        return "0.00"
    if f == 0:
        return "0.00"
    return f"{f:,.2f}"


def fmt_pct(val) -> str:
    """百分率: 'XX.XX%' 形式"""
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
# ネットワークサマリー行変換
# ============================================================

def network_row_to_dict(r: dict) -> dict:
    """APIレスポンス1行をネットワークサマリーのdictに変換する"""
    cmp = r.get("campaign", {})
    seg = r.get("segments", {})
    m   = r.get("metrics", {})

    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions", 0))
    allcv       = float(m.get("allConversions", 0))
    cv_value    = float(m.get("conversionsValue", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))

    network_raw = seg.get("adNetworkType", "UNKNOWN")
    network_ja  = NETWORK_TYPE_MAP.get(network_raw, network_raw)

    return {
        "キャンペーン":              cmp.get("name", ""),
        "キャンペーン ID":           str(cmp.get("id", "")),
        "campaign_type":            cmp.get("advertisingChannelType", ""),
        "ネットワーク":              network_ja,
        "ネットワーク（原文）":       network_raw,
        "表示回数":                  imp,
        "クリック数":                clk,
        "通貨コード":                "JPY",
        "費用":                      str(round(cost_yen)),
        "コンバージョン":            fmt_cv(conv),
        "すべてのコンバージョン":     fmt_cv(allcv),
        "コンバージョン値":          fmt_cv_value(cv_value),
        # 割合は後で計算
        "imp割合":                   "",
        "cost割合":                  "",
        # 照合用（JSON内部のみ）
        "_cost_exact": cost_yen,
        "_conv_exact": conv,
        "_allcv_exact": allcv,
        "_cv_value_exact": cv_value,
        "_imp_raw": imp,
        "_cost_raw": cost_yen,
    }


def calc_network_ratios(rows: list) -> list:
    """キャンペーン単位でimp割合・cost割合を計算する"""
    # キャンペーン別に合計を計算
    campaign_totals = {}
    for r in rows:
        cid = r["キャンペーン ID"]
        if cid not in campaign_totals:
            campaign_totals[cid] = {"imp": 0, "cost": 0.0}
        campaign_totals[cid]["imp"]  += r["_imp_raw"]
        campaign_totals[cid]["cost"] += r["_cost_raw"]

    for r in rows:
        cid = r["キャンペーン ID"]
        totals = campaign_totals[cid]
        if totals["imp"] > 0:
            r["imp割合"] = f"{r['_imp_raw'] / totals['imp'] * 100:.1f}%"
        else:
            r["imp割合"] = "0.0%"
        if totals["cost"] > 0:
            r["cost割合"] = f"{r['_cost_raw'] / totals['cost'] * 100:.1f}%"
        else:
            r["cost割合"] = "0.0%"

    return rows


# ============================================================
# プレースメント詳細行変換
# ============================================================

def placement_row_to_dict(r: dict) -> dict:
    """APIレスポンス1行をプレースメント詳細のdictに変換する"""
    cmp = r.get("campaign", {})
    gpv = r.get("groupPlacementView", {})
    m   = r.get("metrics", {})

    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions", 0))
    allcv       = float(m.get("allConversions", 0))
    cv_value    = float(m.get("conversionsValue", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))

    # CTR, CVR, CPC
    ctr = (clk / imp * 100) if imp > 0 else 0
    cvr = (conv / clk * 100) if clk > 0 else 0
    cpc = round(cost_yen / clk) if clk > 0 else 0

    placement_type_raw = gpv.get("placementType", "UNKNOWN")
    placement_type_ja  = PLACEMENT_TYPE_MAP.get(placement_type_raw, placement_type_raw)

    return {
        "キャンペーン":              cmp.get("name", ""),
        "キャンペーン ID":           str(cmp.get("id", "")),
        "プレースメント":            gpv.get("displayName", gpv.get("placement", "")),
        "プレースメントの種類":       placement_type_ja,
        "プレースメントの種類（原文）": placement_type_raw,
        "プレースメントURL":          gpv.get("targetUrl", ""),
        "表示回数":                  imp,
        "クリック数":                clk,
        "通貨コード":                "JPY",
        "費用":                      str(round(cost_yen)),
        "コンバージョン":            fmt_cv(conv),
        "すべてのコンバージョン":     fmt_cv(allcv),
        "コンバージョン値":          fmt_cv_value(cv_value),
        "クリック率":                fmt_pct(ctr),
        "コンバージョン率":          fmt_pct(cvr),
        "平均クリック単価":          str(cpc),
        # 照合用
        "_cost_exact": cost_yen,
        "_conv_exact": conv,
        "_allcv_exact": allcv,
        "_cv_value_exact": cv_value,
    }


# ============================================================
# サマリー生成
# ============================================================

def build_network_summary(network_rows: list) -> dict:
    """ネットワーク別の集計サマリーを作成"""
    summary = {}
    for r in network_rows:
        nw = r["ネットワーク（原文）"]
        if nw not in summary:
            summary[nw] = {
                "network": r["ネットワーク"],
                "network_raw": nw,
                "impressions": 0,
                "clicks": 0,
                "cost_yen": 0.0,
                "conversions": 0.0,
                "all_conversions": 0.0,
                "conversions_value": 0.0,
            }
        summary[nw]["impressions"]       += r["_imp_raw"]
        summary[nw]["clicks"]            += r["クリック数"]
        summary[nw]["cost_yen"]          += r["_cost_raw"]
        summary[nw]["conversions"]       += r["_conv_exact"]
        summary[nw]["all_conversions"]   += r["_allcv_exact"]
        summary[nw]["conversions_value"] += r["_cv_value_exact"]

    # 割合を計算
    total_imp  = sum(s["impressions"] for s in summary.values())
    total_cost = sum(s["cost_yen"] for s in summary.values())

    for s in summary.values():
        s["imp_share"]  = round(s["impressions"] / total_imp * 100, 1) if total_imp > 0 else 0
        s["cost_share"] = round(s["cost_yen"] / total_cost * 100, 1) if total_cost > 0 else 0
        s["ctr"]        = round(s["clicks"] / s["impressions"] * 100, 2) if s["impressions"] > 0 else 0
        s["cvr"]        = round(s["conversions"] / s["clicks"] * 100, 2) if s["clicks"] > 0 else 0
        s["cpa"]        = round(s["cost_yen"] / s["conversions"]) if s["conversions"] > 0 else None

    # imp_share降順でソート
    result = sorted(summary.values(), key=lambda x: x["imp_share"], reverse=True)
    return result


def build_placement_type_summary(placement_rows: list) -> dict:
    """プレースメントタイプ別の集計サマリーを作成"""
    summary = {}
    for r in placement_rows:
        pt = r.get("プレースメントの種類（原文）", "UNKNOWN")
        if pt not in summary:
            summary[pt] = {
                "placement_type": r["プレースメントの種類"],
                "placement_type_raw": pt,
                "count": 0,
                "impressions": 0,
                "clicks": 0,
                "cost_yen": 0.0,
                "conversions": 0.0,
                "all_conversions": 0.0,
            }
        summary[pt]["count"]           += 1
        summary[pt]["impressions"]     += r["表示回数"]
        summary[pt]["clicks"]          += r["クリック数"]
        summary[pt]["cost_yen"]        += r["_cost_exact"]
        summary[pt]["conversions"]     += r["_conv_exact"]
        summary[pt]["all_conversions"] += r["_allcv_exact"]

    result = sorted(summary.values(), key=lambda x: x["impressions"], reverse=True)
    return result


# ============================================================
# CSV エクスポート
# ============================================================

def export_network_csv(csv_rows: list, out_path: Path,
                       account_name: str, date_from: str, date_to: str):
    """ネットワーク別サマリーCSVをエクスポートする"""
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("プレースメントレポート（ネットワーク別サマリー）\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")
        writer = csv.DictWriter(f, fieldnames=NETWORK_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ ネットワークCSV保存: {out_path}  ({len(csv_rows):,} 行)")


def export_placement_csv(csv_rows: list, out_path: Path,
                         account_name: str, date_from: str, date_to: str):
    """プレースメント詳細CSVをエクスポートする"""
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("プレースメントレポート（配信先詳細）\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")
        writer = csv.DictWriter(f, fieldnames=PLACEMENT_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ プレースメントCSV保存: {out_path}  ({len(csv_rows):,} 行)")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads プレースメントレポート取得")
    parser.add_argument("--site",         required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",         dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",           dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign",     default=None,     help="特定キャンペーンIDで絞り込み")
    parser.add_argument("--summary-only", action="store_true", help="ネットワークサマリーのみ（プレースメント詳細なし）")
    args = parser.parse_args()

    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]
    site_id = account["site_id"]

    # キャンペーン名 → ID 解決
    if args.campaign:
        args.campaign = resolve_campaign_id(site_id, args.campaign)

    print(f"アカウント : {account['name']} ({cid})")
    print(f"期間       : {args.date_from} 〜 {args.date_to}")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")

    token = get_access_token(creds)

    # ── クエリ1: ネットワーク別サマリー ──
    print("\n[1/2] ネットワーク別サマリー取得中...")
    network_raw = fetch_network_summary(creds, token, cid,
                                        args.date_from, args.date_to, args.campaign)
    print(f"  取得行数: {len(network_raw):,} 件")

    network_rows = [network_row_to_dict(r) for r in network_raw]
    network_rows = calc_network_ratios(network_rows)

    # ネットワーク別集計
    network_summary = build_network_summary(network_rows)

    # API合計
    network_totals = {
        "rows":  len(network_rows),
        "imp":   sum(r["_imp_raw"]     for r in network_rows),
        "clk":   sum(r["クリック数"]   for r in network_rows),
        "cost":  sum(r["_cost_raw"]    for r in network_rows),
        "conv":  sum(r["_conv_exact"]  for r in network_rows),
        "allcv": sum(r["_allcv_exact"] for r in network_rows),
    }

    print("\n【ネットワーク別サマリー】")
    print(f"  {'ネットワーク':<25} {'imp':>10} {'imp%':>7} {'cost':>10} {'cost%':>7} {'conv':>7} {'allcv':>7}")
    print("  " + "-" * 80)
    for ns in network_summary:
        print(f"  {ns['network']:<25} {ns['impressions']:>10,} {ns['imp_share']:>6.1f}% "
              f"{ns['cost_yen']:>10,.0f} {ns['cost_share']:>6.1f}% "
              f"{ns['conversions']:>7.1f} {ns['all_conversions']:>7.1f}")

    print(f"\n  合計: imp={network_totals['imp']:,}  clk={network_totals['clk']:,}  "
          f"cost={network_totals['cost']:,.0f}円  conv={network_totals['conv']:.1f}  "
          f"allcv={network_totals['allcv']:.1f}")

    # ── クエリ2: プレースメント詳細 ──
    placement_rows = []
    placement_totals = {}
    placement_type_summary = []

    if not args.summary_only:
        print("\n[2/2] プレースメント詳細取得中...")
        try:
            placement_raw = fetch_placement_detail(creds, token, cid,
                                                   args.date_from, args.date_to, args.campaign)
            print(f"  取得行数: {len(placement_raw):,} 件")

            placement_rows = [placement_row_to_dict(r) for r in placement_raw]

            placement_totals = {
                "rows":  len(placement_rows),
                "imp":   sum(r["表示回数"]      for r in placement_rows),
                "clk":   sum(r["クリック数"]    for r in placement_rows),
                "cost":  sum(r["_cost_exact"]  for r in placement_rows),
                "conv":  sum(r["_conv_exact"]  for r in placement_rows),
                "allcv": sum(r["_allcv_exact"] for r in placement_rows),
            }

            placement_type_summary = build_placement_type_summary(placement_rows)

            print(f"\n【プレースメントタイプ別集計】")
            print(f"  {'タイプ':<25} {'件数':>6} {'imp':>10} {'clk':>8} {'cost':>10}")
            print("  " + "-" * 65)
            for ps in placement_type_summary:
                print(f"  {ps['placement_type']:<25} {ps['count']:>6,} {ps['impressions']:>10,} "
                      f"{ps['clicks']:>8,} {ps['cost_yen']:>10,.0f}")

        except Exception as e:
            error_msg = str(e)
            if "PERMISSION_DENIED" in error_msg or "METRIC_ACCESS_DENIED" in error_msg:
                print(f"  ⚠ プレースメント詳細取得不可: アクセス権限エラー")
                print(f"    group_placement_view はアカウントの権限設定により取得できない場合があります")
            else:
                print(f"  ⚠ プレースメント詳細取得エラー: {error_msg[:200]}")
    else:
        print("\n[2/2] --summary-only: プレースメント詳細をスキップ")

    # ============================================================
    # JSON保存
    # ============================================================
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_out = OUTPUT_DIR / f"{site_id}_placement_{args.date_from}_{args.date_to}.json"

    result = {
        "account":     account,
        "period":      {"from": args.date_from, "to": args.date_to},
        "campaign_id": args.campaign,
        "fetched_at":  datetime.now().isoformat(),
        "network_totals":       network_totals,
        "network_summary":      network_summary,
        "network_rows":         network_rows,
    }

    if placement_rows:
        result["placement_totals"]       = placement_totals
        result["placement_type_summary"] = placement_type_summary
        result["placement_rows"]         = placement_rows

    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    # ============================================================
    # CSV保存
    # ============================================================
    # ネットワークサマリーCSV
    csv_network_out = OUTPUT_DIR / f"{site_id}_placement_network_{args.date_from}_{args.date_to}.csv"
    export_network_csv(network_rows, csv_network_out, account["name"],
                       args.date_from, args.date_to)

    # プレースメント詳細CSV
    if placement_rows:
        csv_placement_out = OUTPUT_DIR / f"{site_id}_placement_detail_{args.date_from}_{args.date_to}.csv"
        export_placement_csv(placement_rows, csv_placement_out, account["name"],
                             args.date_from, args.date_to)


if __name__ == "__main__":
    main()
