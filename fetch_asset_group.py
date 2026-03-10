#!/usr/bin/env python3
"""
Google Ads Pmax アセットグループレポート取得スクリプト
管理画面「アセット グループ レポート」CSVと同じ列構成で出力する
期間集計（period）、日別（daily）、月別（monthly）の3つのモード対応

【使い方】
  # 期間集計（デフォルト）
  python3 fetch_asset_group.py --site 065 --from 2026-01-01 --to 2026-01-31

  # 日別モード
  python3 fetch_asset_group.py --site 065 --from 2026-01-01 --to 2026-01-31 --mode daily

  # 月別モード
  python3 fetch_asset_group.py --site 065 --from 2026-01-01 --to 2026-01-31 --mode monthly

  # 特定キャンペーンのみ
  python3 fetch_asset_group.py --site 065 --from 2026-01-01 --to 2026-01-31 --campaign 23335615195

  # 管理画面CSVと照合
  python3 fetch_asset_group.py --site 065 --from 2026-01-01 --to 2026-01-31 \
      --csv /path/to/065アセット\ グループ\ レポート.csv

【出力列（管理画面CSV互換）】
  Period mode (24 cols):
    アセット グループのステータス, アセット グループ, キャンペーン, 広告見出し,
    長い広告見出し, 説明文, マーケティング画像, スクエアのマーケティング画像,
    縦向きのマーケティング画像, 広告アセットの充実度, ステータス,
    オーディエンス シグナル, 検索テーマ, 表示回数, クリック数, 通貨コード,
    費用, コンバージョン, すべてのコンバージョン,
    アセット グループ ID, キャンペーン ID, 最終ページ URL, パス 1, パス 2

  Daily mode (25 cols): 日付 + 上記24列

  Monthly mode (25 cols): 月 + 上記24列

【モード詳細】
  period (デフォルト): 期間全体の集計値（segments.dateなし）
  daily: 日付ごとの集計（segments.date を GAQL SELECT に追加）
  monthly: 日付を year-month で集計し、メトリクスを合算

【備考】
  - 「ステータス」列: 管理画面では「有効（制限付き）」など詳細表示だが、
    APIからは ENABLED→「有効」/PAUSED→「一時停止中」のみ取得可能
  - 画像列: API から取得した URL
  - オーディエンス シグナル: audience.name を取得
  - 検索テーマ: API v22 では取得困難（取得できない場合は ' --'）
  - 「最終ページ URL」: 管理画面CSVと同様に '[URL]' 形式で出力
  - 管理画面CSVの末尾の「合計:」行は照合時に除外する
"""

import json
import sys
import csv
import io
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path
from campaign_db import resolve_campaign_id

# ============================================================
# パス設定
# ============================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"

# 管理画面CSVと同じ列順（period mode: 24列）
CSV_COLUMNS_BASE = [
    "アセット グループのステータス",
    "アセット グループ",
    "キャンペーン",
    "広告見出し",
    "長い広告見出し",
    "説明文",
    "マーケティング画像",
    "スクエアのマーケティング画像",
    "縦向きのマーケティング画像",
    "広告アセットの充実度",
    "ステータス",
    "オーディエンス シグナル",
    "検索テーマ",
    "表示回数",
    "クリック数",
    "通貨コード",
    "費用",
    "コンバージョン",
    "すべてのコンバージョン",
    "アセット グループ ID",
    "キャンペーン ID",
    "最終ページ URL",
    "パス 1",
    "パス 2",
]

# アセットグループステータス→日本語
STATUS_MAP = {
    "ENABLED": "有効",
    "PAUSED":  "一時停止中",
    "REMOVED": "削除済み",
}

# 広告アセット充実度→日本語
AD_STRENGTH_MAP = {
    "EXCELLENT":   "優良",
    "GOOD":        "良好",
    "FAIR":        "平均",
    "POOR":        "低い",
    "PENDING":     "審査中",
    "NO_ADS":      "広告なし",
    "UNSPECIFIED": " --",
    "UNKNOWN":     " --",
}

# field_type → 対応列名
TEXT_FIELD_MAP = {
    "HEADLINE":      "広告見出し",
    "LONG_HEADLINE": "長い広告見出し",
    "DESCRIPTION":   "説明文",
}

IMAGE_FIELD_MAP = {
    "MARKETING_IMAGE":          "マーケティング画像",
    "SQUARE_MARKETING_IMAGE":   "スクエアのマーケティング画像",
    "PORTRAIT_MARKETING_IMAGE": "縦向きのマーケティング画像",
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
# アセットグループ メトリクス取得
# ============================================================

def fetch_asset_group_metrics(creds: dict, token: str, customer_id: str,
                               date_from: str, date_to: str,
                               mode: str = "period",
                               campaign_id: str = None) -> list:
    """asset_group リソースからメトリクスを取得"""
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    # segments.date を SELECT に入れるかどうか
    segments_select = "segments.date," if mode in ("daily", "monthly") else ""

    gaql = f"""
        SELECT
            {segments_select}
            asset_group.id,
            asset_group.name,
            asset_group.status,
            asset_group.ad_strength,
            asset_group.final_urls,
            asset_group.path1,
            asset_group.path2,
            campaign.id,
            campaign.name,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions_value,
            metrics.all_conversions,
            metrics.conversions
        FROM asset_group
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          AND asset_group.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name, asset_group.name
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# テキストアセット取得（見出し・説明文）
# ============================================================

def fetch_text_assets(creds: dict, token: str, customer_id: str,
                      campaign_id: str = None) -> dict:
    """
    asset_group_asset から HEADLINE/LONG_HEADLINE/DESCRIPTION を取得。
    戻り値: {asset_group_id: {"広告見出し": "text1, text2,...", ...}}
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            asset_group.id,
            asset_group_asset.field_type,
            asset.text_asset.text
        FROM asset_group_asset
        WHERE campaign.status != 'REMOVED'
          AND asset_group.status != 'REMOVED'
          AND asset.type = 'TEXT'
          {campaign_filter}
        ORDER BY asset_group.id
    """
    rows = search_all(creds, token, customer_id, gaql)

    # asset_group_id → {列名: [テキスト,...]} の辞書に集約
    result = {}
    for r in rows:
        ag_id = str(r.get("assetGroup", {}).get("id", ""))
        field_type = r.get("assetGroupAsset", {}).get("fieldType", "")
        text = r.get("asset", {}).get("textAsset", {}).get("text", "")
        col = TEXT_FIELD_MAP.get(field_type)
        if col and text:
            result.setdefault(ag_id, {})
            result[ag_id].setdefault(col, [])
            result[ag_id][col].append(text)

    # リスト → カンマ区切り文字列
    for ag_id in result:
        for col in result[ag_id]:
            result[ag_id][col] = ", ".join(result[ag_id][col])

    return result


# ============================================================
# 画像アセット取得
# ============================================================

def fetch_image_assets(creds: dict, token: str, customer_id: str,
                       campaign_id: str = None) -> dict:
    """
    asset_group_asset から IMAGE を取得。
    戻り値: {asset_group_id: {"マーケティング画像": "url1, url2,...", ...}}
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            asset_group.id,
            asset_group_asset.field_type,
            asset.image_asset.full_size.url
        FROM asset_group_asset
        WHERE campaign.status != 'REMOVED'
          AND asset_group.status != 'REMOVED'
          AND asset.type = 'IMAGE'
          {campaign_filter}
        ORDER BY asset_group.id
    """
    try:
        rows = search_all(creds, token, customer_id, gaql)
    except Exception as e:
        print(f"  ※ 画像アセット取得エラー（スキップ）: {e}")
        return {}

    result = {}
    for r in rows:
        ag_id = str(r.get("assetGroup", {}).get("id", ""))
        field_type = r.get("assetGroupAsset", {}).get("fieldType", "")
        url = r.get("asset", {}).get("imageAsset", {}).get("fullSize", {}).get("url", "")
        col = IMAGE_FIELD_MAP.get(field_type)
        if col and url:
            result.setdefault(ag_id, {})
            result[ag_id].setdefault(col, [])
            result[ag_id][col].append(url)

    for ag_id in result:
        for col in result[ag_id]:
            result[ag_id][col] = ", ".join(result[ag_id][col])

    return result


# ============================================================
# オーディエンス シグナル取得
# ============================================================

def fetch_audience_signals(creds: dict, token: str, customer_id: str,
                            campaign_id: str = None) -> dict:
    """
    asset_group_signal からオーディエンス名を取得。
    戻り値: {asset_group_id: "audience_name"}
    """
    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    gaql = f"""
        SELECT
            asset_group.id,
            asset_group_signal.audience.audience
        FROM asset_group_signal
        WHERE campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY asset_group.id
    """
    try:
        rows = search_all(creds, token, customer_id, gaql)
    except Exception as e:
        print(f"  ※ オーディエンスシグナル取得エラー（スキップ）: {e}")
        return {}

    result = {}
    for r in rows:
        ag_id = str(r.get("assetGroup", {}).get("id", ""))
        aud_signal = r.get("assetGroupSignal", {}).get("audience", {})
        aud_resource = aud_signal.get("audience", "")
        if ag_id and aud_resource:
            result[ag_id] = aud_resource
    return result


# ============================================================
# 値フォーマット
# ============================================================

def fmt_pct(numerator, denominator) -> str:
    if not denominator:
        return "0.00%"
    return f"{numerator / denominator * 100:.2f}%"


def fmt_cpa(cost_yen, conversions) -> str:
    if not conversions:
        return "0"
    return str(round(cost_yen / conversions))


def fmt_conv_value(val) -> str:
    """コンバージョン値: XX,XXX.XX 形式"""
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return "0.00"


# ============================================================
# 1行変換（期間/日別/月別共通）
# ============================================================

def row_to_csv_format(r: dict,
                      text_assets: dict,
                      image_assets: dict,
                      audience_signals: dict,
                      mode: str = "period") -> dict:
    """APIレスポンス1行を管理画面CSV列のdictに変換する"""
    ag      = r.get("assetGroup", {})
    cmp     = r.get("campaign", {})
    m       = r.get("metrics", {})

    ag_id       = str(ag.get("id", ""))
    ag_name     = ag.get("name", "")
    ag_status   = ag.get("status", "UNSPECIFIED")
    ad_strength = ag.get("adStrength", "UNSPECIFIED")

    # 最終ページURL: [URL] 形式
    final_urls = ag.get("finalUrls", [])
    final_url  = f"[{final_urls[0]}]" if final_urls else ""

    # メトリクス
    cost_micros = int(m.get("costMicros", 0))
    cost_yen    = cost_micros / 1_000_000
    conv        = float(m.get("conversions",    0))
    allcv       = float(m.get("allConversions", 0))
    imp         = int(m.get("impressions", 0))
    clk         = int(m.get("clicks", 0))
    conv_value  = float(m.get("conversionsValue", 0))

    # テキストアセット（見出し・説明文）
    ag_texts  = text_assets.get(ag_id, {})
    ag_images = image_assets.get(ag_id, {})
    ag_aud    = audience_signals.get(ag_id, " --")

    result = {
        "アセット グループのステータス": ag_status,
        "アセット グループ":            ag_name,
        "キャンペーン":                 cmp.get("name", ""),
        "広告見出し":                   ag_texts.get("広告見出し",    " --"),
        "長い広告見出し":               ag_texts.get("長い広告見出し", " --"),
        "説明文":                       ag_texts.get("説明文",        " --"),
        "マーケティング画像":            ag_images.get("マーケティング画像",            " --"),
        "スクエアのマーケティング画像":  ag_images.get("スクエアのマーケティング画像",   " --"),
        "縦向きのマーケティング画像":    ag_images.get("縦向きのマーケティング画像",     " --"),
        "広告アセットの充実度":         AD_STRENGTH_MAP.get(ad_strength, ad_strength),
        "ステータス":                   STATUS_MAP.get(ag_status, ag_status),
        "オーディエンス シグナル":       ag_aud,
        "検索テーマ":                   " --",
        "表示回数":                     imp,
        "クリック数":                   clk,
        "通貨コード":                   "JPY",
        "費用":                         str(round(cost_yen)),
        "コンバージョン":               f"{conv:.2f}",
        "すべてのコンバージョン":        f"{allcv:.2f}",
        "アセット グループ ID":         ag_id,
        "キャンペーン ID":              str(cmp.get("id", "")),
        "最終ページ URL":               final_url,
        "パス 1":                       ag.get("path1", ""),
        "パス 2":                       ag.get("path2", ""),
        # 照合用
        "_cost_exact":  cost_yen,
        "_conv_exact":  conv,
        "_allcv_exact": allcv,
    }

    # 日付列の追加（daily/monthly モード）
    if mode in ("daily", "monthly"):
        result["_date"] = r.get("segments", {}).get("date", "")

    return result


# ============================================================
# 月別集計
# ============================================================

def aggregate_by_month(daily_rows: list) -> list:
    """
    daily_rows（各行に_dateを持つ）を year-month で集計する
    同じ asset_group + campaign + month の行を1行に統合
    """
    aggregated = {}
    for row in daily_rows:
        date_str = row.get("_date", "")
        # date_str = "2026-01-15" → month_key = "2026-01"
        month_key = date_str[:7] if len(date_str) >= 7 else date_str

        # キー: (ag_id, campaign_id, month_key)
        ag_id = row.get("アセット グループ ID", "")
        cmp_id = row.get("キャンペーン ID", "")
        key = (ag_id, cmp_id, month_key)

        if key not in aggregated:
            # 初回: ほぼそのままコピー（メトリクスはこれから合算）
            aggregated[key] = dict(row)
            aggregated[key]["_month"] = month_key
        else:
            # メトリクスを合算
            prev = aggregated[key]
            prev["表示回数"] += row["表示回数"]
            prev["クリック数"] += row["クリック数"]
            prev["コンバージョン"] = str(
                float(prev["コンバージョン"]) + float(row["コンバージョン"])
            )
            prev["すべてのコンバージョン"] = str(
                float(prev["すべてのコンバージョン"]) + float(row["すべてのコンバージョン"])
            )
            prev["_cost_exact"] += row["_cost_exact"]
            prev["_conv_exact"] += float(row["コンバージョン"])
            prev["_allcv_exact"] += float(row["すべてのコンバージョン"])

            # 費用を再計算
            prev["費用"] = str(round(prev["_cost_exact"]))

    return list(aggregated.values())


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(csv_rows: list, out_path: Path, date_from: str, date_to: str,
               mode: str = "period"):
    def fmt_date_ja(d: str) -> str:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return f"{dt.year}年{dt.month}月{dt.day}日"
        except ValueError:
            return d

    # mode に応じて列を決定
    if mode == "daily":
        columns = ["日付"] + CSV_COLUMNS_BASE
    elif mode == "monthly":
        columns = ["月"] + CSV_COLUMNS_BASE
    else:
        columns = CSV_COLUMNS_BASE

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("アセット グループ レポート\n")
        f.write(f"{fmt_date_ja(date_from)} - {fmt_date_ja(date_to)}\n")

        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()

        # 行を加工して出力
        for row in csv_rows:
            output_row = dict(row)
            if mode == "daily":
                output_row["日付"] = row.get("_date", "")
            elif mode == "monthly":
                output_row["月"] = row.get("_month", "")
            writer.writerow(output_row)

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
        if v in ("--", " --", ""): return 0.0
        try: return float(v)
        except: return 0.0

    all_rows = list(csv.DictReader(io.StringIO(content)))
    # 「合計:」行（合計: キャンペーン、合計: サポートされていないエンティティ）を除外
    rows = [r for r in all_rows
            if not r.get("アセット グループのステータス", "").startswith("合計")]

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
    parser = argparse.ArgumentParser(description="Google Ads Pmax アセットグループレポート取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--from",     dest="date_from", required=True, help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to",       dest="date_to",   required=True, help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--mode",     default="period", choices=["period", "daily", "monthly"],
                        help="集計モード: period(デフォルト), daily, monthly")
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
    print(f"モード     : {args.mode}")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")

    token = get_access_token(creds)

    # ── アセットグループメトリクス取得 ────────────────────
    print("\nアセットグループ メトリクス取得中...")
    metric_rows = fetch_asset_group_metrics(creds, token, cid,
                                            args.date_from, args.date_to,
                                            mode=args.mode,
                                            campaign_id=args.campaign)
    print(f"  取得行数: {len(metric_rows):,} 件")

    # ── テキストアセット取得 ────────────────────────────
    print("テキストアセット取得中（見出し・説明文）...")
    text_assets = fetch_text_assets(creds, token, cid, args.campaign)
    print(f"  取得アセットグループ数: {len(text_assets):,}")

    # ── 画像アセット取得 ────────────────────────────────
    print("画像アセット取得中...")
    image_assets = fetch_image_assets(creds, token, cid, args.campaign)
    print(f"  取得アセットグループ数: {len(image_assets):,}")

    # ── オーディエンス シグナル取得 ─────────────────────
    print("オーディエンス シグナル取得中...")
    audience_signals = fetch_audience_signals(creds, token, cid, args.campaign)
    print(f"  取得アセットグループ数: {len(audience_signals):,}")

    # ── 行変換 ─────────────────────────────────────────
    print("\n行変換中...")
    csv_rows = [row_to_csv_format(r, text_assets, image_assets, audience_signals, mode=args.mode)
                for r in metric_rows]

    # Monthly mode: 月別に集計
    if args.mode == "monthly":
        print("月別集計中...")
        csv_rows = aggregate_by_month(csv_rows)

    # API統計を計算
    api = {
        "rows":  len(csv_rows),
        "imp":   sum(r["表示回数"]      for r in csv_rows),
        "clk":   sum(r["クリック数"]    for r in csv_rows),
        "cost":  sum(r["_cost_exact"]  for r in csv_rows),
        "conv":  sum(float(r["コンバージョン"]) for r in csv_rows),
        "allcv": sum(float(r["すべてのコンバージョン"]) for r in csv_rows),
    }

    print("\n【API取得結果】")
    print(f"  行数             : {api['rows']:,} 件")
    print(f"  表示回数         : {api['imp']:,}")
    print(f"  クリック数       : {api['clk']:,}")
    print(f"  費用             : {api['cost']:,.0f} 円")
    print(f"  コンバージョン   : {api['conv']}")
    print(f"  全コンバージョン : {api['allcv']}")

    # ── JSON保存 ────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mode_suffix = "" if args.mode == "period" else f"_{args.mode}"
    json_out = OUTPUT_DIR / f"{account['site_id']}_asset_group{mode_suffix}_{args.date_from}_{args.date_to}.json"
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "account":     account,
            "period":      {"from": args.date_from, "to": args.date_to},
            "mode":        args.mode,
            "campaign_id": args.campaign,
            "fetched_at":  datetime.now().isoformat(),
            "api_totals":  api,
            "rows":        csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    # ── CSV保存 ─────────────────────────────────────────
    csv_out = OUTPUT_DIR / f"{account['site_id']}_asset_group{mode_suffix}_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, args.date_from, args.date_to, mode=args.mode)

    # ── 管理画面CSVとの照合 ─────────────────────────────
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        mc = load_csv_totals(args.csv)
        print(f"  管理画面行数（合計行除外）: {mc['rows']:,}  API行数: {api['rows']:,}")
        print(f"  ※ 管理画面CSV末尾の「合計: キャンペーン」「合計: サポートされていないエンティティ」行は除外")
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
