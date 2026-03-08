#!/usr/bin/env python3
"""
Google Ads キャンペーン設定情報取得スクリプト
アカウント内の全キャンペーン（または指定キャンペーン）の設定をCSV/JSONで出力する

【使い方】
  python3 fetch_campaign_settings.py --site 065

  # 特定キャンペーンのみ
  python3 fetch_campaign_settings.py --site 065 --campaign 23335615195

【出力列】
  キャンペーンID, キャンペーン名, ステータス, タイプ,
  1日予算（円）, 入札戦略, 目標CPA（円）,
  開始日, 終了日,
  対象地域, 除外地域, 地域マッチング,
  言語, コンバージョン目標
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

CSV_COLUMNS = [
    "キャンペーンID",
    "キャンペーン名",
    "ステータス",
    "タイプ",
    "1日予算（円）",
    "入札戦略",
    "目標CPA（円）",
    "開始日",
    "終了日",
    "対象地域",
    "除外地域",
    "地域マッチング",
    "言語",
    "コンバージョン目標",
]

# ── マッピング辞書 ──────────────────────────────────────────

CAMPAIGN_STATUS_MAP = {
    "ENABLED": "有効",
    "PAUSED":  "一時停止",
    "REMOVED": "削除済み",
}

CAMPAIGN_TYPE_MAP = {
    "SEARCH":          "検索",
    "DISPLAY":         "ディスプレイ",
    "SHOPPING":        "ショッピング",
    "VIDEO":           "動画",
    "PERFORMANCE_MAX": "Pmax",
    "SMART":           "スマート",
    "LOCAL":           "ローカル",
    "DEMAND_GEN":      "デマンドジェネレーション",
}

BIDDING_MAP = {
    "MAXIMIZE_CONVERSIONS":       "コンバージョン数の最大化",
    "MAXIMIZE_CONVERSION_VALUE":  "コンバージョン値の最大化",
    "TARGET_CPA":                 "目標CPA",
    "TARGET_ROAS":                "目標ROAS",
    "MANUAL_CPC":                 "手動CPC",
    "MANUAL_CPM":                 "手動CPM",
    "MANUAL_CPV":                 "手動CPV",
    "TARGET_SPEND":               "目標インプレッションシェア",
    "PERCENT_CPC":                "エンhanced CPC",
    "ENHANCED_CPC":               "拡張CPC",
    "COMMISSION":                 "コミッション",
}

GEO_MATCH_MAP = {
    "LOCATION_OF_PRESENCE": "所在地",
    "AREA_OF_INTEREST":     "インタレスト（所在地 + 関心）",
    "DONT_CARE":            "指定なし",
    "UNSPECIFIED":          " --",
}

CONV_GOAL_MAP = {
    "MAXIMIZE_CONVERSION_VALUE": "コンバージョン値の最大化",
    "MAXIMIZE_CONVERSIONS":      "コンバージョン数の最大化",
}


# ============================================================
# 設定ファイルの読み込み
# ============================================================

def load_credentials():
    if not CREDENTIALS_FILE.exists():
        print(f"エラー: {CREDENTIALS_FILE} が見つかりません"); sys.exit(1)
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)["google_ads"]

def load_account(site_query: str) -> dict:
    if not ACCOUNTS_FILE.exists():
        print(f"エラー: {ACCOUNTS_FILE} が見つかりません"); sys.exit(1)
    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)["accounts"]
    q = site_query.strip()
    matched = [a for a in accounts
               if a.get("site_id") == q or a.get("name") == q
               or a.get("customer_id") == q
               or a["customer_id"].replace("-", "") == q.replace("-", "")]
    if not matched:
        print(f"エラー: '{site_query}' に一致するアカウントが見つかりません"); sys.exit(1)
    return matched[0]


# ============================================================
# 認証
# ============================================================

def get_access_token(creds: dict) -> str:
    res = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     creds["oauth"]["client_id"],
        "client_secret": creds["oauth"]["client_secret"],
        "refresh_token": creds["oauth"]["refresh_token"],
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
    results, page_token = [], None
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
# クエリ 1: キャンペーン基本情報 + 予算 + 入札
# ============================================================

def fetch_campaigns(creds, token, customer_id, campaign_id=None) -> list:
    cf = f"AND campaign.id = {campaign_id}" if campaign_id else ""
    gaql = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.bidding_strategy_type,
            campaign.maximize_conversions.target_cpa_micros,
            campaign.maximize_conversion_value.target_roas,
            campaign.target_cpa.target_cpa_micros,
            campaign.start_date,
            campaign.end_date,
            campaign.geo_target_type_setting.positive_geo_target_type,
            campaign.optimization_goal_setting.optimization_goal_types,
            campaign_budget.amount_micros,
            campaign_budget.delivery_method
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          {cf}
        ORDER BY campaign.name
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# クエリ 2: 地域ターゲット (LOCATION + PROXIMITY)
# ============================================================

def fetch_geo_target_names(creds, token, customer_id, resource_names: list) -> dict:
    """
    geo_target_constant リソース名のリストから {resource_name: "地名"} を返す
    例: ["geoTargetConstants/20137", ...] → {"geoTargetConstants/20137": "Sapporo"}
    """
    if not resource_names:
        return {}
    # IDを抽出して IN句で一括取得
    ids = []
    for rn in resource_names:
        parts = rn.split("/")
        if parts:
            ids.append(parts[-1])
    if not ids:
        return {}
    ids_str = ", ".join(ids)
    gaql = f"""
        SELECT
            geo_target_constant.resource_name,
            geo_target_constant.name,
            geo_target_constant.country_code
        FROM geo_target_constant
        WHERE geo_target_constant.id IN ({ids_str})
    """
    rows = search_all(creds, token, customer_id, gaql)
    result = {}
    for r in rows:
        gtc  = r.get("geoTargetConstant", {})
        rn   = gtc.get("resourceName", "")
        name = gtc.get("name", "")
        cc   = gtc.get("countryCode", "")
        label = f"{name}（{cc}）" if cc and cc != "JP" else name
        result[rn] = label
    return result


def fetch_geo_criteria(creds, token, customer_id, campaign_id=None) -> dict:
    """
    {campaign_id: {"対象地域": [...], "除外地域": [...]}} を返す
    """
    cf = f"AND campaign.id = {campaign_id}" if campaign_id else ""

    # LOCATION（市区町村・都道府県等）: geo_target_constant は別クエリで取得
    gaql_loc = f"""
        SELECT
            campaign.id,
            campaign_criterion.negative,
            campaign_criterion.location.geo_target_constant
        FROM campaign_criterion
        WHERE campaign_criterion.type = 'LOCATION'
          AND campaign.status != 'REMOVED'
          {cf}
    """
    # PROXIMITY（半径ターゲット）
    gaql_prox = f"""
        SELECT
            campaign.id,
            campaign_criterion.negative,
            campaign_criterion.proximity.address.city_name,
            campaign_criterion.proximity.address.street_address,
            campaign_criterion.proximity.radius,
            campaign_criterion.proximity.radius_units
        FROM campaign_criterion
        WHERE campaign_criterion.type = 'PROXIMITY'
          AND campaign.status != 'REMOVED'
          {cf}
    """

    # --- LOCATION 取得 ---
    loc_rows = search_all(creds, token, customer_id, gaql_loc)

    # geo_target_constant の名前を一括取得
    geo_rns = list({
        r.get("campaignCriterion", {}).get("location", {}).get("geoTargetConstant", "")
        for r in loc_rows
        if r.get("campaignCriterion", {}).get("location", {}).get("geoTargetConstant")
    })
    geo_names = fetch_geo_target_names(creds, token, customer_id, geo_rns)

    result = {}
    for r in loc_rows:
        cid    = str(r.get("campaign", {}).get("id", ""))
        cc     = r.get("campaignCriterion", {})
        geo_rn = cc.get("location", {}).get("geoTargetConstant", "")
        label  = geo_names.get(geo_rn, geo_rn)
        is_neg = cc.get("negative", False)
        result.setdefault(cid, {"対象地域": [], "除外地域": []})
        key = "除外地域" if is_neg else "対象地域"
        result[cid][key].append(label)

    # --- PROXIMITY 取得 ---
    try:
        for r in search_all(creds, token, customer_id, gaql_prox):
            cid  = str(r.get("campaign", {}).get("id", ""))
            cc   = r.get("campaignCriterion", {})
            prox = cc.get("proximity", {})
            addr = prox.get("address", {})
            city = addr.get("cityName", "")
            st   = addr.get("streetAddress", "")
            rad  = prox.get("radius", "")
            unit = "km" if prox.get("radiusUnits") == "KILOMETERS" else "mi"
            label = f"{city} {st}から半径{rad}{unit}".strip()
            is_neg = cc.get("negative", False)
            result.setdefault(cid, {"対象地域": [], "除外地域": []})
            key = "除外地域" if is_neg else "対象地域"
            result[cid][key].append(label)
    except Exception as e:
        print(f"  ※ PROXIMITY 取得エラー（スキップ）: {e}")

    return result


# ============================================================
# クエリ 3: 言語ターゲット
# ============================================================

def fetch_language_criteria(creds, token, customer_id, campaign_id=None) -> dict:
    """
    {campaign_id: ["日本語", ...]} を返す
    """
    cf = f"AND campaign.id = {campaign_id}" if campaign_id else ""
    gaql = f"""
        SELECT
            campaign.id,
            language_constant.name
        FROM campaign_criterion
        WHERE campaign_criterion.type = 'LANGUAGE'
          AND campaign.status != 'REMOVED'
          {cf}
    """
    result = {}
    for r in search_all(creds, token, customer_id, gaql):
        cid  = str(r.get("campaign", {}).get("id", ""))
        name = r.get("languageConstant", {}).get("name", "")
        result.setdefault(cid, [])
        if name:
            result[cid].append(name)
    return result


# ============================================================
# 1行変換
# ============================================================

def row_to_csv(r: dict, geo: dict, lang: dict) -> dict:
    cmp    = r.get("campaign", {})
    budget = r.get("campaignBudget", {})

    cid_str = str(cmp.get("id", ""))

    # 予算
    budget_micros = int(budget.get("amountMicros", 0))
    budget_yen    = round(budget_micros / 1_000_000)

    # 入札戦略
    bidding_type = cmp.get("biddingStrategyType", "")
    bidding_ja   = BIDDING_MAP.get(bidding_type, bidding_type)

    # 目標CPA: maximize_conversions か target_cpa のどちらかに入っている
    target_cpa_m = (
        int(cmp.get("maximizeConversions", {}).get("targetCpaMicros", 0) or 0)
        or int(cmp.get("targetCpa", {}).get("targetCpaMicros", 0) or 0)
    )
    target_cpa_yen = round(target_cpa_m / 1_000_000) if target_cpa_m else ""

    # 開始日・終了日
    start = cmp.get("startDate", "")
    end   = cmp.get("endDate", "") or "未設定"

    # 地域
    geo_info  = geo.get(cid_str, {})
    pos_geos  = ", ".join(geo_info.get("対象地域", [])) or " --"
    neg_geos  = ", ".join(geo_info.get("除外地域", [])) or " --"

    # 地域マッチング
    pos_type  = cmp.get("geoTargetTypeSetting", {}).get("positiveGeoTargetType", "")
    geo_match = GEO_MATCH_MAP.get(pos_type, pos_type or " --")

    # 言語
    languages = ", ".join(lang.get(cid_str, [])) or " --"

    # コンバージョン目標
    goals = cmp.get("optimizationGoalSetting", {}).get("optimizationGoalTypes", [])
    goals_ja = ", ".join(CONV_GOAL_MAP.get(g, g) for g in goals) if goals else "アカウントのデフォルト"

    return {
        "キャンペーンID":     cid_str,
        "キャンペーン名":     cmp.get("name", ""),
        "ステータス":         CAMPAIGN_STATUS_MAP.get(cmp.get("status", ""), cmp.get("status", "")),
        "タイプ":             CAMPAIGN_TYPE_MAP.get(cmp.get("advertisingChannelType", ""), cmp.get("advertisingChannelType", "")),
        "1日予算（円）":      budget_yen,
        "入札戦略":           bidding_ja,
        "目標CPA（円）":      target_cpa_yen,
        "開始日":             start,
        "終了日":             end,
        "対象地域":           pos_geos,
        "除外地域":           neg_geos,
        "地域マッチング":     geo_match,
        "言語":               languages,
        "コンバージョン目標": goals_ja,
    }


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(rows: list, out_path: Path, account_name: str):
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(f"キャンペーン設定レポート\n")
        f.write(f"取得日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"✓ CSV保存: {out_path}  ({len(rows):,} 行)")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads キャンペーン設定取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--campaign", default=None,  help="特定キャンペーンIDまたはキャンペーン名で絞り込み")
    # run_fetcher.sh との互換性のため --from / --to も受け付けるが使用しない
    parser.add_argument("--from",  dest="date_from", default=None)
    parser.add_argument("--to",    dest="date_to",   default=None)
    args = parser.parse_args()

    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]

    if args.campaign:
        args.campaign = resolve_campaign_id(account["site_id"], args.campaign)

    print(f"アカウント : {account['name']} ({cid})")
    if args.campaign:
        print(f"キャンペーン: {args.campaign}")

    token = get_access_token(creds)

    # ── 各種情報を取得 ──────────────────────────────────────
    print("\nキャンペーン基本情報取得中...")
    campaigns = fetch_campaigns(creds, token, cid, args.campaign)
    print(f"  取得件数: {len(campaigns):,}")

    print("地域ターゲット取得中...")
    geo = fetch_geo_criteria(creds, token, cid, args.campaign)

    print("言語ターゲット取得中...")
    lang = fetch_language_criteria(creds, token, cid, args.campaign)

    # ── 行変換 ─────────────────────────────────────────────
    csv_rows = [row_to_csv(r, geo, lang) for r in campaigns]

    # ── 表示 ───────────────────────────────────────────────
    print(f"\n【キャンペーン設定一覧】{len(csv_rows)} 件")
    for row in csv_rows:
        print(f"\n  ■ {row['キャンペーン名']} (ID: {row['キャンペーンID']})")
        for k in CSV_COLUMNS[2:]:
            val = str(row.get(k, ""))
            if val and val != " --":
                print(f"    {k}: {val}")

    # ── JSON保存 ────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_out = OUTPUT_DIR / f"{account['site_id']}_campaign_settings_{ts}.json"
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "account":    account,
            "fetched_at": datetime.now().isoformat(),
            "rows":       csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    # ── CSV保存 ─────────────────────────────────────────────
    csv_out = OUTPUT_DIR / f"{account['site_id']}_campaign_settings_{ts}.csv"
    export_csv(csv_rows, csv_out, account["name"])


if __name__ == "__main__":
    main()
