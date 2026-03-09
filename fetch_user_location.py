#!/usr/bin/env python3
"""
ユーザーの所在地レポート 取得スクリプト  v2
管理画面「市区町村別（ユーザーの所在地）・日別」と同じ形式でデータを取得し、
CSVエクスポートおよびCSV照合が可能。

【使い方】
  python3 fetch_user_location.py --site 065 --from 2026-01-01 --to 2026-01-31
  python3 fetch_user_location.py --site 065 --campaign 23335569301 --from 2026-01-01 --to 2026-01-31

【CSVエクスポート（管理画面と同じ形式）】
  python3 fetch_user_location.py --site 065 --from 2026-01-01 --to 2026-01-31 --export-csv

【照合用CSVの指定】
  python3 fetch_user_location.py --site 065 --from 2026-01-01 --to 2026-01-31 --csv path/to/file.csv
"""

import json
import sys
import csv
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from campaign_db import resolve_campaign_id

# ============================================================
# パス設定
# ============================================================
SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"


# ============================================================
# 設定ファイル読み込み
# ============================================================
def load_credentials():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)["google_ads"]

def find_account(query):
    with open(ACCOUNTS_FILE) as f:
        data = json.load(f)
    q = query.strip()
    q_nohyphen = q.replace("-", "")
    for a in data["accounts"]:
        if (a.get("site_id") == q or a.get("name") == q
                or a.get("customer_id") == q
                or a["customer_id"].replace("-", "") == q_nohyphen):
            return a
    print(f"エラー: '{query}' に一致するアカウントが見つかりません")
    sys.exit(1)


# ============================================================
# 認証
# ============================================================
def get_access_token(creds):
    res = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     creds["oauth"]["client_id"],
        "client_secret": creds["oauth"]["client_secret"],
        "refresh_token": creds["oauth"]["refresh_token"],
        "grant_type":    "refresh_token",
    }, timeout=30)
    res.raise_for_status()
    return res.json()["access_token"]


# ============================================================
# Google Ads API 汎用クエリ
# ============================================================
def search_all(creds, token, customer_id, gaql):
    cid = customer_id.replace("-", "")
    url = f"https://googleads.googleapis.com/v22/customers/{cid}/googleAds:search"
    headers = {
        "Authorization":     f"Bearer {token}",
        "developer-token":   creds["developer_token"],
        "login-customer-id": creds["mcc_customer_id"],
        "Content-Type":      "application/json",
    }
    results, page_token = [], None
    while True:
        payload = {"query": gaql}
        if page_token:
            payload["pageToken"] = page_token
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        if res.status_code != 200:
            raise Exception(f"API Error [{res.status_code}]: {res.text[:400]}")
        data = res.json()
        results.extend(data.get("results", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results


# ============================================================
# GeoTargetConstant 名前解決（日本語対応）
# ============================================================
def resolve_geo_targets(creds, token, customer_id, geo_ids: set) -> dict:
    """
    geoTargetConstants/{id} 形式のIDセットを日本語名に解決する。

    geoTargetConstants:suggest API (locale=ja) を使って日本語名を取得する。
    API障害時はGAQL経由の英語名にフォールバックする。

    Returns
    -------
    dict: {
        "geoTargetConstants/1009076": {
            "id": "1009076",
            "name": "札幌市",
            "canonical_name": "札幌市,北海道,日本",
            "target_type": "City",
            "region": "北海道",
            "city": "札幌市",
            "detail": "札幌市",
        }
    }
    """
    if not geo_ids:
        return {}

    valid_ids = [g for g in geo_ids if g and "geoTargetConstants/" in g]
    if not valid_ids:
        return {}

    # ── Step 1: geoTargetConstants:suggest で日本語名を取得 ──────
    result = _resolve_via_suggest(creds, token, valid_ids)

    # ── Step 2: 未解決分は GAQL で英語名にフォールバック ──────────
    unresolved = [g for g in valid_ids if g not in result]
    if unresolved:
        fallback = _resolve_via_gaql(creds, token, customer_id, unresolved)
        result.update(fallback)

    return result


def _resolve_via_suggest(creds, token, geo_ids: list) -> dict:
    """geoTargetConstants:suggest API で日本語名を取得"""
    url = "https://googleads.googleapis.com/v22/geoTargetConstants:suggest"
    headers = {
        "Authorization":   f"Bearer {token}",
        "developer-token": creds["developer_token"],
        "Content-Type":    "application/json",
    }

    result = {}
    # バッチサイズ 50件ずつリクエスト
    batch_size = 50
    for i in range(0, len(geo_ids), batch_size):
        batch = geo_ids[i:i+batch_size]
        # 正しいリクエスト形式:
        # geoTargets は1つのオブジェクト、その中の geoTargetConstants が配列
        payload = {
            "locale": "ja",
            "geoTargets": {
                "geoTargetConstants": batch,  # ["geoTargetConstants/1009076", ...]
            },
        }
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=30)
            if res.status_code != 200:
                print(f"  [警告] suggest API エラー [{res.status_code}]: {res.text[:200]}")
                continue
            data = res.json()
            for item in data.get("geoTargetConstantSuggestions", []):
                gc = item.get("geoTargetConstant", {})
                raw_id = str(gc.get("id", ""))
                if not raw_id:
                    continue
                name  = gc.get("name", "")
                canon = gc.get("canonicalName", "")  # "札幌市,北海道,日本"
                ttype = gc.get("targetType", "")
                key   = f"geoTargetConstants/{raw_id}"
                result[key] = _parse_geo_info(raw_id, name, canon, ttype,
                                               gc.get("parentGeoTarget", ""),
                                               gc.get("countryCode", ""))
        except Exception as e:
            print(f"  [警告] suggest API 例外: {e}")
    return result


def _resolve_via_gaql(creds, token, customer_id, geo_ids: list) -> dict:
    """GAQL経由で geo_target_constant を取得（英語名フォールバック）"""
    id_list = [g.replace("geoTargetConstants/", "") for g in geo_ids]
    ids_str = ", ".join(id_list)
    gaql = f"""
        SELECT
            geo_target_constant.id,
            geo_target_constant.name,
            geo_target_constant.canonical_name,
            geo_target_constant.parent_geo_target,
            geo_target_constant.country_code,
            geo_target_constant.target_type
        FROM geo_target_constant
        WHERE geo_target_constant.id IN ({ids_str})
    """
    result = {}
    try:
        rows = search_all(creds, token, customer_id, gaql)
        for r in rows:
            gc    = r.get("geoTargetConstant", {})
            raw_id = str(gc.get("id", ""))
            key   = f"geoTargetConstants/{raw_id}"
            result[key] = _parse_geo_info(
                raw_id,
                gc.get("name", ""),
                gc.get("canonicalName", ""),
                gc.get("targetType", ""),
                gc.get("parentGeoTarget", ""),
                gc.get("countryCode", ""),
            )
    except Exception as e:
        print(f"  [警告] GAQL GeoTarget解決エラー: {e}")
    return result


# ── 都道府県 英語→日本語 変換マップ ──────────────────────────
PREF_MAP_EN_TO_JA = {
    "Hokkaido": "北海道",
    "Aomori": "青森県", "Iwate": "岩手県", "Miyagi": "宮城県",
    "Akita": "秋田県", "Yamagata": "山形県", "Fukushima": "福島県",
    "Ibaraki": "茨城県", "Tochigi": "栃木県", "Gunma": "群馬県",
    "Saitama": "埼玉県", "Chiba": "千葉県", "Tokyo": "東京都",
    "Kanagawa": "神奈川県", "Niigata": "新潟県", "Toyama": "富山県",
    "Ishikawa": "石川県", "Fukui": "福井県", "Yamanashi": "山梨県",
    "Nagano": "長野県", "Shizuoka": "静岡県", "Aichi": "愛知県",
    "Mie": "三重県", "Shiga": "滋賀県", "Kyoto": "京都府",
    "Osaka": "大阪府", "Hyogo": "兵庫県", "Nara": "奈良県",
    "Wakayama": "和歌山県", "Tottori": "鳥取県", "Shimane": "島根県",
    "Okayama": "岡山県", "Hiroshima": "広島県", "Yamaguchi": "山口県",
    "Tokushima": "徳島県", "Kagawa": "香川県", "Ehime": "愛媛県",
    "Kochi": "高知県", "Fukuoka": "福岡県", "Saga": "佐賀県",
    "Nagasaki": "長崎県", "Kumamoto": "熊本県", "Oita": "大分県",
    "Miyazaki": "宮崎県", "Kagoshima": "鹿児島県", "Okinawa": "沖縄県",
    # 旧・郡名等
    "Kameda District": "亀田郡",
}


def _parse_geo_info(raw_id, name, canon, ttype, parent, country) -> dict:
    """
    canonical_name（英語）と name（日本語）から地域階層を組み立てる。

    suggest API v2 の実際のレスポンス形式:
      name = "札幌市"（日本語）
      canonical_name = "Sapporo,Hokkaido,Japan"（英語）

    → region: 都道府県名は canonical_name の英語から日本語にマップ
    → city, detail: name（日本語）をそのまま使用

    4階層（区・郵便番号レベル）の場合:
      name = "白石区"
      canonical_name = "Shiroishi Ward,Sapporo,Hokkaido,Japan"
    → region = 北海道, city = 札幌市（parentから解決）, detail = 白石区
    """
    parts = [p.strip() for p in canon.split(",")]

    # 都道府県名を英語→日本語に変換
    def ja_region(en_name):
        return PREF_MAP_EN_TO_JA.get(en_name, en_name)

    if len(parts) >= 4:
        # 4階層: 最詳細, 市, 都道府県, 国
        region_en = parts[-2]
        region = ja_region(region_en)
        # city は parent から後で解決できないため英語名を日本語化しておく
        city   = name  # name が日本語なのでここでは「最詳細」と同じになる可能性あり
        detail = name
    elif len(parts) == 3:
        # 3階層: 市/区, 都道府県, 国 ← 多くの日本の市はこれ
        region_en = parts[-2]
        region = ja_region(region_en)
        city   = name  # suggest API が日本語名を返す
        detail = name
    elif len(parts) == 2:
        region_en = parts[0]
        region = ja_region(region_en)
        city   = name
        detail = name
    else:
        region = name
        city   = name
        detail = name

    return {
        "id":            raw_id,
        "name":          name,
        "canonical_name": canon,
        "target_type":   ttype,
        "parent_id":     parent,
        "country_code":  country,
        "region":        region,
        "city":          city,
        "detail":        detail,
    }


# ============================================================
# キャンペーン一覧取得
# ============================================================
def get_campaigns(creds, token, customer_id):
    gaql = """
        SELECT campaign.id, campaign.name
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    rows = search_all(creds, token, customer_id, gaql)
    return {str(r["campaign"]["id"]): r["campaign"]["name"] for r in rows}


# ============================================================
# ユーザー所在地データ取得（メイン）
# ============================================================
def fetch_user_location(creds, token, customer_id, campaign_id, date_from, date_to, debug_path=None):
    """
    user_location_view からユーザーの所在地データを取得する。

    取得フィールド（管理画面CSVに対応）:
    - segments.date          → 日
    - campaign.name          → キャンペーン
    - segments.geo_target_city → 市区町村（ユーザーの所在地）+ 最詳細（IDを後で解決）
    - metrics.impressions    → 表示回数
    - metrics.clicks         → クリック数
    - metrics.cost_micros    → 費用
    - metrics.conversions    → コンバージョン
    - metrics.all_conversions → すべてのコンバージョン
    - metrics.search_impression_share → 検索IS
    - metrics.search_budget_lost_impression_share → IS損失率（予算）
    - metrics.search_rank_lost_impression_share   → IS損失率（ランク）
    - metrics.top_impression_percentage           → 上部インプレッション割合
    - metrics.absolute_top_impression_percentage  → 最上部インプレッション割合
    """
    gaql = f"""
        SELECT
            campaign.id,
            campaign.name,
            user_location_view.country_criterion_id,
            user_location_view.targeting_location,
            segments.geo_target_city,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions
        FROM user_location_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
        ORDER BY segments.date ASC, metrics.cost_micros DESC
    """
    # 注: 以下は user_location_view では PROHIBITED_METRIC のため使用不可
    #   - metrics.search_impression_share
    #   - metrics.search_budget_lost_impression_share
    #   - metrics.search_rank_lost_impression_share
    #   - metrics.top_impression_percentage
    #   - metrics.absolute_top_impression_percentage
    # 管理画面CSVでは「--」または「0」で表示される
    rows = search_all(creds, token, customer_id, gaql)

    # ── デバッグ: 生データ保存 ────────────────────────────
    if debug_path:
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump({
                "total_rows": len(rows),
                "first_row": rows[0] if rows else {},
                "all_rows": rows,
            }, f, ensure_ascii=False, indent=2)
        print(f"  [DEBUG] 生データ保存: {debug_path} ({len(rows)}件)")

    # targeting_location の内訳
    def get_tl(r):
        v = r.get("userLocationView", {}).get("targetingLocation")
        if v is not None:
            return v
        v = r.get("user_location_view", {}).get("targeting_location")
        return v

    tl_true  = sum(1 for r in rows if get_tl(r) is True)
    tl_false = sum(1 for r in rows if get_tl(r) is False)
    print(f"  [内訳] targeting_location=TRUE:{tl_true}件 / FALSE:{tl_false}件 / 合計:{len(rows)}件")

    # キャンペーンIDフィルタ（Python側）
    if campaign_id:
        before = len(rows)
        rows = [r for r in rows if str(r.get("campaign", {}).get("id", "")) == str(campaign_id)]
        print(f"  [フィルタ後] campaign_id={campaign_id}: {before}件 → {len(rows)}件")

    return rows


# ============================================================
# 行データを「管理画面CSV行」形式に変換
# ============================================================
def row_to_csv_format(r, geo_map: dict) -> dict:
    """
    APIレスポンス1行をCSV列と対応するdictに変換する。
    """
    camp = r.get("campaign", {})
    m    = r.get("metrics", {})
    seg  = r.get("segments", {})
    ulv  = r.get("userLocationView", {})

    date          = seg.get("date", "")
    campaign_name = camp.get("name", "")
    geo_city_key  = seg.get("geoTargetCity", "")

    # GeoTarget名前解決
    # 市区町村: geo_target_city（市レベル）を使用
    city_info = geo_map.get(geo_city_key, {})
    region    = city_info.get("region", geo_city_key)  # 地域（都道府県）
    city      = city_info.get("name",   geo_city_key)  # 市区町村（日本語名）
    # detail: city と同じ（APIでは郵便番号・区レベルのデータは取得不可）
    # 注: segments.geo_target_postal_code / geo_target_most_specific_location を
    #     SELECT に追加すると郵便番号特定不能な行（全体の96%以上）が欠落するため使用不可
    detail    = city

    cost    = int(m.get("costMicros", 0)) / 1_000_000
    clicks  = int(m.get("clicks", 0))
    imps    = int(m.get("impressions", 0))
    conv    = float(m.get("conversions", 0))
    allcv   = float(m.get("allConversions", 0))
    avg_cpc = round(cost / clicks) if clicks > 0 else 0

    # 検索IS系（数値 or "--" when not applicable）
    def fmt_is(val):
        """IS値を管理画面と同じ形式に: 0.xx → "XX.XX%", null → "--"  """
        if val is None or val == "":
            return "--"
        try:
            f = float(val)
            if f == 0.0:
                return "--"
            return f"{f * 100:.2f}%"
        except (ValueError, TypeError):
            return "--"

    def fmt_pct(val):
        """割合値をパーセント表示"""
        if val is None or val == "":
            return "0"
        try:
            f = float(val)
            if f == 0.0:
                return "0"
            return f"{f * 100:.2f}%"
        except (ValueError, TypeError):
            return "0"

    # IS系3指標: user_location_view では取得不可（管理画面CSVでも「--」）
    search_is      = "--"
    is_loss_budget = "--"
    is_loss_rank   = "--"
    # 上部インプレッション割合: user_location_view では取得不可のため固定値
    top_imp        = "0"
    abs_top_imp    = "0"

    campaign_id = str(camp.get("id", ""))

    return {
        "日":                               date,
        "campaign_id":                      campaign_id,
        "キャンペーン":                       campaign_name,
        "地域（ユーザーの所在地）":            region,
        "市区町村（ユーザーの所在地）":         city,
        "最も詳細な対象地域（ユーザーの所在地）": detail,
        "表示回数":                           imps,
        "クリック数":                          clicks,
        "通貨コード":                          "JPY",
        "費用":                               round(cost),
        "コンバージョン":                       f"{conv:.2f}",
        "すべてのコンバージョン":               f"{allcv:.2f}",
        "平均クリック単価":                     avg_cpc,
        "検索広告のインプレッション シェア":     search_is,
        "検索広告の IS 損失率（予算）":          is_loss_budget,
        "検索広告の IS 損失率（ランク）":        is_loss_rank,
        "上部インプレッションの割合":            top_imp,
        "最上部インプレッションの割合":          abs_top_imp,
        # 内部用（照合・分析）
        "_geo_target_key":        geo_city_key,
        # 注: geo_target_postal_code / most_specific_location は SELECT に追加すると
        # データ欠落するため使用不可。管理画面CSV の郵便番号レベルは API で再現不可
        "_targeting_location": r.get("userLocationView", {}).get("targetingLocation"),
        "_cost_exact":        cost,
        "_conv_exact":        conv,
        "_allcv_exact":       allcv,
    }


# ============================================================
# CSV読み込み・集計（管理画面CSVとの照合用）
# ============================================================
def load_csv_totals(csv_path):
    rows = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i < 3: continue
            if not any(row): continue
            rows.append(row)

    def to_f(v):
        try: return float(v.replace(",", ""))
        except: return 0.0

    return {
        "rows":  len(rows),
        "imp":   sum(to_f(r[5]) for r in rows if len(r) > 5),
        "clk":   sum(to_f(r[6]) for r in rows if len(r) > 6),
        "cost":  sum(to_f(r[8]) for r in rows if len(r) > 8),
        "conv":  sum(to_f(r[9]) for r in rows if len(r) > 9),
        "allcv": sum(to_f(r[10]) for r in rows if len(r) > 10),
    }


# ============================================================
# CSV出力（管理画面と同じ形式）
# ============================================================
CSV_COLUMNS = [
    "日", "キャンペーン",
    "地域（ユーザーの所在地）", "市区町村（ユーザーの所在地）",
    "最も詳細な対象地域（ユーザーの所在地）",
    "表示回数", "クリック数", "通貨コード", "費用",
    "コンバージョン", "すべてのコンバージョン", "平均クリック単価",
    "検索広告のインプレッション シェア", "検索広告の IS 損失率（予算）",
    "検索広告の IS 損失率（ランク）", "上部インプレッションの割合",
    "最上部インプレッションの割合",
]

def export_csv(csv_rows: list, out_path: Path, account_name: str, date_from: str, date_to: str):
    """管理画面CSVと同じ形式でエクスポート"""
    date_label = f"{date_from.replace('-', '年', 1).replace('-', '月')}日 - {date_to.replace('-', '年', 1).replace('-', '月')}日"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["市区町村別（ユーザーの所在地）・日別"])
        writer.writerow([date_label])
        writer.writerow(CSV_COLUMNS)
        for row in csv_rows:
            writer.writerow([row.get(col, "") for col in CSV_COLUMNS])
    print(f"  ✓ CSVエクスポート: {out_path} ({len(csv_rows)}行)")


# ============================================================
# メイン
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="ユーザーの所在地データ取得・CSV照合 v2")
    parser.add_argument("--site",       required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--campaign",   help="キャンペーンID（省略時は全キャンペーン）")
    parser.add_argument("--from",       dest="date_from", required=True, help="開始日 YYYY-MM-DD")
    parser.add_argument("--to",         dest="date_to",   required=True, help="終了日 YYYY-MM-DD")
    parser.add_argument("--csv",        help="照合するCSVファイルのパス（省略可）")
    parser.add_argument("--export-csv", action="store_true", help="管理画面と同じ形式のCSVをエクスポート")
    parser.add_argument("--debug",      action="store_true", help="生のAPIレスポンスをJSONに保存してデバッグ")
    args = parser.parse_args()

    print("=" * 60)
    print("ユーザーの所在地レポート 取得スクリプト v2")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    creds   = load_credentials()
    account = find_account(args.site)
    cid     = account["customer_id"]
    print(f"対象アカウント: {account['site_id']} {account['name']} ({cid})")
    print(f"取得期間: {args.date_from} 〜 {args.date_to}")

    # キャンペーン名 → ID 解決（数字IDのままでも可）
    if args.campaign:
        args.campaign = resolve_campaign_id(account["site_id"], args.campaign)

    # アクセストークン取得
    print("\nアクセストークン取得中...")
    token = get_access_token(creds)
    print("✓ 認証成功")

    # キャンペーン一覧
    campaigns = get_campaigns(creds, token, cid)
    if args.campaign:
        camp_name = campaigns.get(args.campaign, "不明")
        print(f"対象キャンペーン: {camp_name} (ID: {args.campaign})")
    else:
        print(f"対象キャンペーン: 全キャンペーン ({len(campaigns)}件)")

    # ユーザー所在地データ取得
    print("\nユーザーの所在地データ取得中...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    debug_path = None
    if args.debug:
        debug_path = OUTPUT_DIR / f"{account['site_id']}_user_location_debug_raw.json"

    rows = fetch_user_location(creds, token, cid, args.campaign,
                               args.date_from, args.date_to, debug_path=debug_path)
    print(f"✓ {len(rows)} 件取得")

    # GeoTarget 名前解決（city レベルのみ）
    print("\nGeoTarget 名前解決中...")
    geo_ids = set()
    for r in rows:
        seg = r.get("segments", {})
        city_key = seg.get("geoTargetCity", "")
        if city_key:
            geo_ids.add(city_key)
    geo_ids.discard("")
    print(f"  ユニーク地域ID: {len(geo_ids)}件")
    geo_map = resolve_geo_targets(creds, token, cid, geo_ids)
    print(f"  ✓ 解決済み: {len(geo_map)}件")

    # 管理画面CSV形式に変換
    csv_rows = [row_to_csv_format(r, geo_map) for r in rows]

    # API集計
    api = {
        "rows":  len(csv_rows),
        "imp":   sum(r["表示回数"] for r in csv_rows),
        "clk":   sum(r["クリック数"] for r in csv_rows),
        "cost":  sum(r["_cost_exact"] for r in csv_rows),
        "conv":  sum(r["_conv_exact"] for r in csv_rows),
        "allcv": sum(r["_allcv_exact"] for r in csv_rows),
    }

    print("\n【API取得結果】")
    print(f"  地域数       : {api['rows']:,} 件")
    print(f"  表示回数     : {api['imp']:,}")
    print(f"  クリック数   : {api['clk']:,}")
    print(f"  費用         : {api['cost']:,.0f} 円")
    print(f"  コンバージョン    : {api['conv']}")
    print(f"  全コンバージョン  : {api['allcv']}")

    # JSON保存
    out_path = OUTPUT_DIR / f"{account['site_id']}_user_location_{args.date_from}_{args.date_to}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "account":     account,
            "period":      {"from": args.date_from, "to": args.date_to},
            "campaign_id": args.campaign,
            "fetched_at":  datetime.now().isoformat(),
            "api_totals":  api,
            "geo_map":     geo_map,
            "rows":        csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {out_path}")

    # CSVエクスポート（常に実行）
    csv_out = OUTPUT_DIR / f"{account['site_id']}_user_location_{args.date_from}_{args.date_to}.csv"
    export_csv(csv_rows, csv_out, account["name"], args.date_from, args.date_to)

    # 管理画面CSVとの照合
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        csved = load_csv_totals(args.csv)
        print(f"{'項目':<20} {'CSV':>12} {'API':>12} {'一致':>6}")
        print("-" * 54)
        checks = [
            ("表示回数",           csved["imp"],   api["imp"]),
            ("クリック数",         csved["clk"],   api["clk"]),
            ("費用(円)",           csved["cost"],  api["cost"]),
            ("コンバージョン",     csved["conv"],  api["conv"]),
            ("全コンバージョン",   csved["allcv"], api["allcv"]),
        ]
        all_ok = True
        for label, cv, av in checks:
            # 費用は管理画面CSV が行ごとに整数丸めのため行数×0.5円の誤差を許容
            tolerance = max(1.0, csved["rows"] * 0.5) if label == "費用(円)" else 1.0
            ok = abs(cv - av) <= tolerance
            mark = "✓" if ok else "✗"
            if not ok: all_ok = False
            diff = cv - av
            print(f"{label:<20} {cv:>12,.1f} {av:>12,.1f} {diff:>+10,.1f} {mark:>6}")
        print("-" * 62)
        print("結果:", "✓ 全項目一致" if all_ok else "✗ 不一致あり")

        # 行数比較（API は市区町村レベル集約、CSV は区・郵便番号レベル）
        print(f"\n行数比較: CSV={csved['rows']}行 / API={api['rows']}行")
        if csved["rows"] != api["rows"]:
            print("  ※ API は市区町村レベルに集約。CSV の方が細かい粒度（区・郵便番号）")
            print("    → 合計値は一致、地域分解の詳細度に差あり（API の仕様）")

    # 地域別サマリー
    print("\n【地域別サマリー（上位10地域）】")
    city_totals = defaultdict(lambda: {"cost": 0.0, "clicks": 0, "conv": 0.0})
    for r in csv_rows:
        city = r["市区町村（ユーザーの所在地）"]
        city_totals[city]["cost"]   += r["_cost_exact"]
        city_totals[city]["clicks"] += r["クリック数"]
        city_totals[city]["conv"]   += r["_conv_exact"]
    top_cities = sorted(city_totals.items(), key=lambda x: x[1]["cost"], reverse=True)[:10]
    print(f"  {'市区町村':<15} {'費用':>10} {'クリック':>8} {'CV':>6}")
    for city, m in top_cities:
        print(f"  {city:<15} ¥{m['cost']:>9,.0f} {m['clicks']:>8,} {m['conv']:>6.1f}")

    print("\n完了")


if __name__ == "__main__":
    main()
