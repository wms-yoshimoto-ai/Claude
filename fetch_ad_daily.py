#!/usr/bin/env python3
"""
Google Ads 広告別・日別データ取得スクリプト
管理画面「広告のパフォーマンス・日別」CSVと同じ列構成で出力する

【使い方】
  python3 fetch_ad_daily.py --site 065 --from 2026-01-01 --to 2026-01-31

  # 特定キャンペーンのみ
  python3 fetch_ad_daily.py --site 065 --from 2026-01-01 --to 2026-01-31 --campaign 23335569301

  # 管理画面CSVと照合
  python3 fetch_ad_daily.py --site 065 --from 2026-01-01 --to 2026-01-31 \
      --csv /path/to/065広告のパフォーマンス・日別.csv

【出力列（管理画面CSV互換・62列）】
  日, 広告の種類, 最終ページ URL,
  広告見出し 1〜15 + 位置 (30列),
  説明文 1〜4 + 位置 (8列),
  パス 1, パス 2, モバイルの最終ページ URL,
  トラッキング テンプレート, 最終ページ URL のサフィックス,
  カスタム パラメータ, 広告 ID, キャンペーン, 広告グループ,
  広告の最終ページ URL, 広告の状態,
  クリック数, 表示回数, クリック率, 通貨コード, 費用,
  コンバージョン, コンバージョン単価, コンバージョン率,
  すべてのコンバージョン, 平均クリック単価

【備考】
  - RSA以外の広告種別（拡張テキスト広告など）は見出し/説明文列が空欄になる
  - 管理画面CSVでは3列目の見出しが "タイトル 3" という表記になっている（CSV互換のためそのまま使用）
  - 位置フィールド: 未固定= ' --', 固定= '見出し 1' / '見出し 2' / '見出し 3' / '説明文 1' / '説明文 2'
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

# 管理画面CSVと同じ列順（62列）
# ※ 3番目の見出しのコンテンツ列は管理画面CSVでは "タイトル 3" という名前
CSV_COLUMNS = [
    "日",
    "広告の種類",
    "最終ページ URL",
    "広告見出し 1",
    "広告見出し 1 の位置",
    "広告見出し 2",
    "広告見出し 2 の位置",
    "タイトル 3",                # 管理画面CSVに合わせて "タイトル 3"（= 広告見出し 3）
    "広告見出し 3 の位置",
    "広告見出し 4",
    "広告見出し 4 の位置",
    "広告見出し 5",
    "広告見出し 5 の位置",
    "広告見出し 6",
    "広告見出し 6 の位置",
    "広告見出し 7",
    "広告見出し 7 の位置",
    "広告見出し 8",
    "広告見出し 8 の位置",
    "広告見出し 9",
    "広告見出し 9 の位置",
    "広告見出し 10",
    "広告見出し 10 の位置",
    "広告見出し 11",
    "広告見出し 11 の位置",
    "広告見出し 12",
    "広告見出し 12 の位置",
    "広告見出し 13",
    "広告見出し 13 の位置",
    "広告見出し 14",
    "広告見出し 14 の位置",
    "広告見出し 15",
    "広告見出し 15 の位置",
    "説明文 1",
    "説明文 1 の位置",
    "説明文 2",
    "説明文 2 の位置",
    "説明文 3",
    "説明文 3 の位置",
    "説明文 4",
    "説明文 4 の位置",
    "パス 1",
    "パス 2",
    "モバイルの最終ページ URL",
    "トラッキング テンプレート",
    "最終ページ URL のサフィックス",
    "カスタム パラメータ",
    "広告 ID",
    "キャンペーン",
    "広告グループ",
    "広告の最終ページ URL",
    "広告の状態",
    "クリック数",
    "表示回数",
    "クリック率",
    "通貨コード",
    "費用",
    "コンバージョン",
    "コンバージョン単価",
    "コンバージョン率",
    "すべてのコンバージョン",
    "平均クリック単価",
]

# 広告タイプ→日本語
AD_TYPE_MAP = {
    "RESPONSIVE_SEARCH_AD":  "レスポンシブ検索広告",
    "EXPANDED_TEXT_AD":      "拡張テキスト広告",
    "CALL_AD":               "コール広告",
    "IMAGE_AD":              "イメージ広告",
    "VIDEO_AD":              "動画広告",
    "RESPONSIVE_DISPLAY_AD": "レスポンシブ ディスプレイ広告",
}

# 広告ステータス→日本語
AD_STATUS_MAP = {
    "ENABLED": "有効",
    "PAUSED":  "一時停止",
    "REMOVED": "削除済み",
}

# pinnedField→管理画面表示
PINNED_MAP = {
    "HEADLINE_1":    "見出し 1",
    "HEADLINE_2":    "見出し 2",
    "HEADLINE_3":    "見出し 3",
    "DESCRIPTION_1": "説明文 1",
    "DESCRIPTION_2": "説明文 2",
    "UNSPECIFIED":   " --",
    "UNKNOWN":       " --",
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
# 広告日別データ取得
# ============================================================

def fetch_ad_daily_data(creds: dict, token: str, customer_id: str,
                        date_from: str, date_to: str,
                        campaign_id: str = None) -> list:
    """
    ad_group_ad リソースから日別広告データを取得する。
    見出し・説明文・パス・メトリクスを含む。
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.type,
            ad_group_ad.ad.final_urls,
            ad_group_ad.ad.mobile_final_urls,
            ad_group_ad.ad.tracking_url_template,
            ad_group_ad.ad.final_url_suffix,
            ad_group_ad.ad.url_custom_parameters,
            ad_group_ad.ad.responsive_search_ad.headlines,
            ad_group_ad.ad.responsive_search_ad.descriptions,
            ad_group_ad.ad.responsive_search_ad.path1,
            ad_group_ad.ad.responsive_search_ad.path2,
            ad_group_ad.status,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions,
            metrics.average_cpc
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
          AND ad_group_ad.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, ad_group.name, ad_group_ad.ad.id, segments.date
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# 値フォーマット
# ============================================================

def fmt_pct(numerator, denominator) -> str:
    """クリック率・コンバージョン率: XX.XX%"""
    if not denominator:
        return "0.00%"
    return f"{numerator / denominator * 100:.2f}%"


def fmt_cpa(cost_yen, conversions) -> str:
    """コンバージョン単価: 整数"""
    if not conversions:
        return "0"
    return str(round(cost_yen / conversions))


def fmt_avg_cpc(cost_micros, clicks) -> str:
    """平均クリック単価: 整数（円）"""
    if not clicks:
        return "0"
    return str(round(cost_micros / 1_000_000 / clicks))


def fmt_pinned(pinned_field: str) -> str:
    """pinnedField → 管理画面表示文字列"""
    return PINNED_MAP.get(pinned_field or "UNSPECIFIED", " --")


def fmt_custom_params(params: list) -> str:
    """url_custom_parameters → カスタム パラメータ文字列"""
    if not params:
        return ""
    # 例: {_param1=value1}{_param2=value2}
    parts = []
    for p in params:
        key = p.get("key", "")
        val = p.get("value", "")
        parts.append(f"{{_{key}={val}}}")
    return "".join(parts)


# ============================================================
# 1行変換
# ============================================================

def row_to_csv_format(r: dict) -> dict:
    """APIレスポンス1行を管理画面CSV列のdictに変換する"""
    cmp      = r.get("campaign", {})
    ag       = r.get("adGroup", {})
    aga      = r.get("adGroupAd", {})
    ad       = aga.get("ad", {})
    rsa      = ad.get("responsiveSearchAd", {})
    seg      = r.get("segments", {})
    m        = r.get("metrics", {})

    # メトリクス
    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions",    0))
    allcv       = float(m.get("allConversions", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))
    avg_cpc_m   = int(m.get("averageCpc", 0))

    # 広告タイプ・ステータス
    ad_type   = AD_TYPE_MAP.get(ad.get("type", ""), ad.get("type", ""))
    ad_status = AD_STATUS_MAP.get(aga.get("status", ""), aga.get("status", ""))

    # 最終ページURL
    final_urls = ad.get("finalUrls", [])
    final_url  = final_urls[0] if final_urls else ""
    mobile_final_urls = ad.get("mobileFinalUrls", [])
    mobile_url = mobile_final_urls[0] if mobile_final_urls else ""

    # トラッキングテンプレート・サフィックス
    tracking   = ad.get("trackingUrlTemplate", "") or " --"
    suffix     = ad.get("finalUrlSuffix", "") or " --"
    custom_p   = fmt_custom_params(ad.get("urlCustomParameters", []))

    # RSA見出し（最大15）
    headlines = rsa.get("headlines", [])
    hl_texts  = [""] * 15
    hl_pins   = [" --"] * 15
    for i, h in enumerate(headlines[:15]):
        hl_texts[i] = h.get("text", "")
        hl_pins[i]  = fmt_pinned(h.get("pinnedField"))

    # RSA説明文（最大4）
    descs     = rsa.get("descriptions", [])
    desc_texts = [" --"] * 4
    desc_pins  = [" --"] * 4
    for i, d in enumerate(descs[:4]):
        desc_texts[i] = d.get("text", "")
        desc_pins[i]  = fmt_pinned(d.get("pinnedField"))

    # パス
    path1 = rsa.get("path1", "") or ""
    path2 = rsa.get("path2", "") or ""

    result = {
        "日":                     seg.get("date", ""),
        "広告の種類":              ad_type,
        "最終ページ URL":          final_url,
        "広告見出し 1":            hl_texts[0],
        "広告見出し 1 の位置":     hl_pins[0],
        "広告見出し 2":            hl_texts[1],
        "広告見出し 2 の位置":     hl_pins[1],
        "タイトル 3":              hl_texts[2],   # 管理画面CSVに合わせて "タイトル 3"
        "広告見出し 3 の位置":     hl_pins[2],
        "広告見出し 4":            hl_texts[3],
        "広告見出し 4 の位置":     hl_pins[3],
        "広告見出し 5":            hl_texts[4],
        "広告見出し 5 の位置":     hl_pins[4],
        "広告見出し 6":            hl_texts[5],
        "広告見出し 6 の位置":     hl_pins[5],
        "広告見出し 7":            hl_texts[6],
        "広告見出し 7 の位置":     hl_pins[6],
        "広告見出し 8":            hl_texts[7],
        "広告見出し 8 の位置":     hl_pins[7],
        "広告見出し 9":            hl_texts[8],
        "広告見出し 9 の位置":     hl_pins[8],
        "広告見出し 10":           hl_texts[9],
        "広告見出し 10 の位置":    hl_pins[9],
        "広告見出し 11":           hl_texts[10],
        "広告見出し 11 の位置":    hl_pins[10],
        "広告見出し 12":           hl_texts[11],
        "広告見出し 12 の位置":    hl_pins[11],
        "広告見出し 13":           hl_texts[12],
        "広告見出し 13 の位置":    hl_pins[12],
        "広告見出し 14":           hl_texts[13],
        "広告見出し 14 の位置":    hl_pins[13],
        "広告見出し 15":           hl_texts[14],
        "広告見出し 15 の位置":    hl_pins[14],
        "説明文 1":                desc_texts[0],
        "説明文 1 の位置":         desc_pins[0],
        "説明文 2":                desc_texts[1],
        "説明文 2 の位置":         desc_pins[1],
        "説明文 3":                desc_texts[2],
        "説明文 3 の位置":         desc_pins[2],
        "説明文 4":                desc_texts[3],
        "説明文 4 の位置":         desc_pins[3],
        "パス 1":                  path1,
        "パス 2":                  path2,
        "モバイルの最終ページ URL": mobile_url,
        "トラッキング テンプレート": tracking,
        "最終ページ URL のサフィックス": suffix,
        "カスタム パラメータ":     custom_p,
        "広告 ID":                 str(ad.get("id", "")),
        "キャンペーン":            cmp.get("name", ""),
        "広告グループ":            ag.get("name", ""),
        "広告の最終ページ URL":    final_url,   # 管理画面CSVでは "最終ページ URL" と同値
        "広告の状態":              ad_status,
        "クリック数":              clk,
        "表示回数":                imp,
        "クリック率":              fmt_pct(clk, imp),
        "通貨コード":              "JPY",
        "費用":                    str(round(cost_yen)),
        "コンバージョン":          f"{conv:.2f}",
        "コンバージョン単価":      fmt_cpa(cost_yen, conv),
        "コンバージョン率":        fmt_pct(conv, clk),
        "すべてのコンバージョン":   f"{allcv:.2f}",
        "平均クリック単価":        fmt_avg_cpc(cost_micros, clk),
        # 照合用（表示しない）
        "_cost_exact":  cost_yen,
        "_conv_exact":  conv,
        "_allcv_exact": allcv,
    }
    return result


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(csv_rows: list, out_path: Path,
               date_from: str, date_to: str):
    """管理画面CSVと同じ形式でエクスポートする"""
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("広告のパフォーマンス・日別\n")
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
        "imp":   sum(safe(r.get("表示回数", 0))              for r in rows),
        "clk":   sum(safe(r.get("クリック数", 0))            for r in rows),
        "cost":  sum(safe(r.get("費用", 0))                 for r in rows),
        "conv":  sum(safe(r.get("コンバージョン", 0))        for r in rows),
        "allcv": sum(safe(r.get("すべてのコンバージョン", 0)) for r in rows),
    }


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads 広告別・日別データ取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",     dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",       dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign", default=None,     help="特定キャンペーンIDまたはキャンペーン名で絞り込み")
    parser.add_argument("--csv",      default=None,     help="管理画面CSVパス（照合用）")
    args = parser.parse_args()

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

    token = get_access_token(creds)

    print("\n広告日別データ取得中...")
    rows = fetch_ad_daily_data(creds, token, cid,
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
    json_out = OUTPUT_DIR / f"{account['site_id']}_ad_daily_{args.date_from}_{args.date_to}.json"
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

    # CSV保存
    csv_out = OUTPUT_DIR / f"{account['site_id']}_ad_daily_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, args.date_from, args.date_to)

    # 管理画面CSVとの照合
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        mc = load_csv_totals(args.csv)
        print(f"  管理画面行数: {mc['rows']:,}  API行数: {api['rows']:,}")
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
            tol  = max(1.0, mc["rows"] * 0.5) if "費用" in label else 0.01
            ok   = "✓" if abs(diff) <= tol else "✗"
            if ok == "✗":
                all_ok = False
            print(f"  {label:<25} {cv:>14,.2f} {av:>14,.2f} {diff:>10,.2f}  {ok}")

        print()
        if all_ok:
            print("  ✓ 全指標が一致しました（許容誤差内）")
        else:
            print("  ✗ 一致しない指標があります")


if __name__ == "__main__":
    main()
