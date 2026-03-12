#!/usr/bin/env python3
"""
Google Ads Pmax AG別CV検索語句マッピングスクリプト

【概要】
  Pmaxキャンペーンの「どのアセットグループ(AG)のCVが、どの検索語句から来たか」を推定する。
  - search_term_insight (日別): 日別の検索カテゴリ別CV
  - asset_group (日別): 日別のAG別CV
  この2データをクロスリファレンスし、1日に1つのAGしかCVしていなければ「確定」、
  複数AGのCVがあればCV値の一致で「推定」とマッピングする。

【使い方】
  python3 fetch_ag_cv_search_terms.py --site 072 --from 2025-12-17 --to 2026-03-09
  python3 fetch_ag_cv_search_terms.py --site 072 --from 2025-12-17 --to 2026-03-09 --campaign 23367869010
  python3 fetch_ag_cv_search_terms.py --site 072 --from 2025-12-17 --to 2026-03-09 --no-db

【出力】
  JSON: {site}_ag_cv_search_terms_{date_from}_{date_to}.json
  CSV:  {site}_ag_cv_search_terms_{date_from}_{date_to}.csv

【confidence レベル】
  - 確定: その日にCVしたAGが1つのみ → 全CV検索語句をそのAGに帰属
  - 確定（症状キーワード）: 検索語句がAG名のキーワードと一致
  - 推定（CV値一致）: 複数AGにCVがあるが、検索語句のCV値と一致するAGが1つ
  - 推定（複数候補）: 複数AGにCVがあり、CV値一致でも特定できない
  - N/A: AG日別データにCVがあるが検索語句データにCVなし（アトリビューション差異）
"""

import json
import sys
import csv
import argparse
import requests
from datetime import datetime
from math import isnan
from pathlib import Path
from collections import defaultdict

try:
    from campaign_db import resolve_campaign_id, list_campaigns
except ImportError:
    resolve_campaign_id = None
    list_campaigns = None

# ============================================================
# パス設定
# ============================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"

CSV_COLUMNS = [
    "日付", "キャンペーン", "キャンペーン ID",
    "AG名", "AG_ID",
    "検索語句", "カテゴリID",
    "CV数", "confidence", "note",
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


def load_account(site_query):
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

def get_access_token(creds):
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

def search_all(creds, token, customer_id, gaql):
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
# Pmaxキャンペーン一覧取得
# ============================================================

def fetch_pmax_campaigns(creds, token, customer_id):
    gaql = """
        SELECT campaign.id, campaign.name
        FROM campaign
        WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
          AND campaign.status != 'REMOVED'
    """
    rows = search_all(creds, token, customer_id, gaql)
    return [{"id": str(r["campaign"]["id"]), "name": r["campaign"]["name"]} for r in rows]


# ============================================================
# Step 1: search_term_insight (日別) 取得
# ============================================================

def fetch_daily_search_term_insight(creds, token, customer_id, campaign_id, date_from, date_to):
    """campaign_search_term_insight の日別データを取得する"""
    gaql = f"""
        SELECT
            segments.date,
            campaign.name,
            campaign.id,
            campaign_search_term_insight.category_label,
            campaign_search_term_insight.id,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions
        FROM campaign_search_term_insight
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.id = {campaign_id}
        ORDER BY segments.date, metrics.impressions DESC
    """
    results = search_all(creds, token, customer_id, gaql)

    # 日別にグループ化
    daily = defaultdict(list)
    for r in results:
        seg  = r.get("segments", {})
        cmp  = r.get("campaign", {})
        ins  = r.get("campaignSearchTermInsight", {})
        m    = r.get("metrics", {})
        date = seg.get("date", "")
        conv = float(m.get("conversions", 0))
        imp  = int(m.get("impressions", 0))
        clk  = int(m.get("clicks", 0))
        label = ins.get("categoryLabel", "")
        if not label:
            label = "（未分類）"

        daily[date].append({
            "campaign_name": cmp.get("name", ""),
            "campaign_id":   str(cmp.get("id", "")),
            "search_category": label,
            "category_id":   ins.get("id", ""),
            "impressions":   imp,
            "clicks":        clk,
            "conversions":   conv,
        })
    return daily


# ============================================================
# Step 2: asset_group (日別) 取得
# ============================================================

def fetch_daily_asset_group(creds, token, customer_id, campaign_id, date_from, date_to):
    """AG日別データ（asset_group）を取得する"""
    gaql = f"""
        SELECT
            segments.date,
            asset_group.name,
            asset_group.id,
            campaign.name,
            campaign.id,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions
        FROM asset_group
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.id = {campaign_id}
          AND asset_group.status != 'REMOVED'
        ORDER BY segments.date, asset_group.name
    """
    results = search_all(creds, token, customer_id, gaql)

    # 日別にグループ化
    daily = defaultdict(list)
    for r in results:
        seg = r.get("segments", {})
        ag  = r.get("assetGroup", {})
        cmp = r.get("campaign", {})
        m   = r.get("metrics", {})
        date = seg.get("date", "")
        conv = float(m.get("conversions", 0))

        daily[date].append({
            "ag_name":       ag.get("name", ""),
            "ag_id":         str(ag.get("id", "")),
            "campaign_name": cmp.get("name", ""),
            "campaign_id":   str(cmp.get("id", "")),
            "impressions":   int(m.get("impressions", 0)),
            "clicks":        int(m.get("clicks", 0)),
            "cost_micros":   int(m.get("costMicros", 0)),
            "conversions":   conv,
            "all_conversions": float(m.get("allConversions", 0)),
        })
    return daily


# ============================================================
# Step 3: クロスリファレンス（マッピング）
# ============================================================

def cross_reference(daily_search_terms, daily_asset_groups, campaign_name, campaign_id):
    """日別の検索語句とAGデータをクロスリファレンスしてマッピングを作成する"""
    mapping = []

    # 全日付の和集合
    all_dates = sorted(set(list(daily_search_terms.keys()) + list(daily_asset_groups.keys())))

    for date in all_dates:
        st_rows = daily_search_terms.get(date, [])
        ag_rows = daily_asset_groups.get(date, [])

        # CVがあるAGを抽出
        cv_ags = [ag for ag in ag_rows if ag["conversions"] > 0]
        # CVがある検索カテゴリを抽出
        cv_terms = [st for st in st_rows if st["conversions"] > 0]

        if not cv_ags:
            # この日にCVなし → スキップ
            continue

        if not cv_terms:
            # AGにはCVがあるが検索語句にCVなし（アトリビューション差異）
            for ag in cv_ags:
                mapping.append({
                    "日付": date,
                    "キャンペーン": campaign_name,
                    "キャンペーン ID": campaign_id,
                    "AG名": ag["ag_name"],
                    "AG_ID": ag["ag_id"],
                    "検索語句": "（検索語句レベルでCV表示なし）",
                    "カテゴリID": "",
                    "CV数": ag["conversions"],
                    "confidence": "N/A",
                    "note": "AG日別データにCVあるが検索語句にCV表示なし（アトリビューション差異）",
                })
            continue

        if len(cv_ags) == 1:
            # 1つのAGだけがCVしている → 確定マッピング
            ag = cv_ags[0]
            for st in cv_terms:
                mapping.append({
                    "日付": date,
                    "キャンペーン": campaign_name,
                    "キャンペーン ID": campaign_id,
                    "AG名": ag["ag_name"],
                    "AG_ID": ag["ag_id"],
                    "検索語句": st["search_category"],
                    "カテゴリID": st.get("category_id", ""),
                    "CV数": st["conversions"],
                    "confidence": "確定",
                    "note": "この日唯一のCV AG",
                })
        else:
            # 複数AGにCVがある → CV値一致で推定 or キーワードマッチ
            for st in cv_terms:
                matched = _match_term_to_ag(st, cv_ags)
                mapping.append({
                    "日付": date,
                    "キャンペーン": campaign_name,
                    "キャンペーン ID": campaign_id,
                    "AG名": matched["ag_name"],
                    "AG_ID": matched["ag_id"],
                    "検索語句": st["search_category"],
                    "カテゴリID": st.get("category_id", ""),
                    "CV数": st["conversions"],
                    "confidence": matched["confidence"],
                    "note": matched["note"],
                })

    return mapping


def _match_term_to_ag(search_term, cv_ags):
    """検索語句を最適なAGにマッチさせる"""
    st_conv = search_term["conversions"]
    st_label = search_term["search_category"].lower()

    # 1. AG名のキーワードが検索語句に含まれるか（症状キーワードマッチ）
    for ag in cv_ags:
        ag_keywords = _extract_ag_keywords(ag["ag_name"])
        if any(kw in st_label for kw in ag_keywords if len(kw) >= 2):
            return {
                "ag_name": ag["ag_name"],
                "ag_id": ag["ag_id"],
                "confidence": "確定（症状キーワード）",
                "note": f"AG名キーワード一致",
            }

    # 2. CV値が一致するAGが1つだけか
    cv_matched = [ag for ag in cv_ags if abs(ag["conversions"] - st_conv) < 0.01]
    if len(cv_matched) == 1:
        return {
            "ag_name": cv_matched[0]["ag_name"],
            "ag_id": cv_matched[0]["ag_id"],
            "confidence": "推定（CV値一致）",
            "note": "",
        }

    # 3. 複数候補 → CV値が最も近いAGを選択
    if cv_matched:
        best = cv_matched[0]
    else:
        best = min(cv_ags, key=lambda ag: abs(ag["conversions"] - st_conv))
    return {
        "ag_name": best["ag_name"],
        "ag_id": best["ag_id"],
        "confidence": "推定（複数候補）",
        "note": f"候補AG: {', '.join(ag['ag_name'] for ag in cv_ags)}",
    }


def _extract_ag_keywords(ag_name):
    """AG名から症状・ターゲットキーワードを抽出する

    例: "AG_c 出っ歯（072）25/12/17" → ["出っ歯"]
        "AG_a 20代（072）25/12/17" → ["20代"]
        "AG_c 口ゴボ" → ["口ゴボ", "口ごぼ"]
    """
    import re
    # "AG_X KEYWORD（...）" からキーワード部分を抽出
    match = re.match(r"AG_\w+\s+(.+?)(?:（|$)", ag_name)
    if not match:
        return []
    keyword = match.group(1).strip()
    keywords = [keyword.lower()]

    # よくある表記揺れの追加
    variant_map = {
        "口ゴボ": ["口ごぼ", "口 ゴボ"],
        "出っ歯": ["出っ歯", "でっぱ"],
        "八重歯": ["八重歯", "やえば"],
        "受け口": ["受け口", "うけくち"],
        "叢生": ["叢生", "そうせい", "歯並び"],
        "開咬": ["開咬", "かいこう", "オープンバイト"],
        "すきっ歯": ["すきっ歯", "隙間"],
    }
    for base, variants in variant_map.items():
        if base.lower() in keyword.lower():
            keywords.extend(v.lower() for v in variants)

    return keywords


# ============================================================
# 出力
# ============================================================

def build_output(mapping, meta):
    """JSON出力用データを構築する"""
    return {
        "meta": meta,
        "mapping": mapping,
    }


def export_csv(mapping, out_path):
    """CSVファイルに出力する"""
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(mapping)
    print(f"  CSV出力: {out_path} ({len(mapping)} 行)")


# ============================================================
# メイン処理
# ============================================================

def run(site_id, date_from, date_to, campaign_id=None, no_db=False):
    """メイン処理: API取得 → クロスリファレンス → 出力"""
    print(f"=== AG別CV検索語句マッピング ===")
    print(f"  site: {site_id}, period: {date_from} ~ {date_to}")

    # 設定読み込み
    creds = load_credentials()
    account = load_account(site_id)
    customer_id = account["customer_id"]
    token = get_access_token(creds)

    # Pmaxキャンペーン特定
    if campaign_id:
        pmax_camps = [{"id": campaign_id, "name": ""}]
        # 名前を取得
        gaql = f"SELECT campaign.name, campaign.id FROM campaign WHERE campaign.id = {campaign_id}"
        res = search_all(creds, token, customer_id, gaql)
        if res:
            pmax_camps[0]["name"] = res[0]["campaign"]["name"]
    else:
        pmax_camps = fetch_pmax_campaigns(creds, token, customer_id)

    if not pmax_camps:
        print("  Pmaxキャンペーンが見つかりません")
        return

    print(f"  Pmaxキャンペーン: {len(pmax_camps)} 件")

    all_mapping = []

    for camp in pmax_camps:
        cid = camp["id"]
        cname = camp["name"]
        print(f"\n  [{cname}] (ID: {cid})")

        # Step 1: search_term_insight (日別)
        print(f"    検索語句インサイト取得中...")
        daily_st = fetch_daily_search_term_insight(creds, token, customer_id, cid, date_from, date_to)
        st_dates = len(daily_st)
        st_total = sum(len(v) for v in daily_st.values())
        print(f"    → {st_dates} 日, {st_total} カテゴリ")

        # Step 2: asset_group (日別)
        print(f"    AG日別データ取得中...")
        daily_ag = fetch_daily_asset_group(creds, token, customer_id, cid, date_from, date_to)
        ag_dates = len(daily_ag)
        ag_total = sum(len(v) for v in daily_ag.values())
        print(f"    → {ag_dates} 日, {ag_total} AG日")

        # Step 3: クロスリファレンス
        print(f"    クロスリファレンス実行中...")
        mapping = cross_reference(daily_st, daily_ag, cname, cid)
        print(f"    → マッピング: {len(mapping)} 行")

        # confidence 内訳
        conf_counts = defaultdict(int)
        for m in mapping:
            conf_counts[m["confidence"]] += 1
        for conf, cnt in sorted(conf_counts.items()):
            print(f"      {conf}: {cnt} 行")

        all_mapping.extend(mapping)

    # メタ情報
    meta = {
        "site_id": site_id,
        "campaign": pmax_camps[0]["name"] if len(pmax_camps) == 1 else f"{len(pmax_camps)} Pmaxキャンペーン",
        "campaign_id": pmax_camps[0]["id"] if len(pmax_camps) == 1 else ",".join(c["id"] for c in pmax_camps),
        "period": f"{date_from} ～ {date_to}",
        "description": "日別検索語句×AG CVマッピング（API自動分析）",
        "extracted_at": datetime.now().strftime("%Y-%m-%d"),
        "total_mappings": len(all_mapping),
    }

    # JSON出力
    output_data = build_output(all_mapping, meta)
    json_path = OUTPUT_DIR / f"{site_id}_ag_cv_search_terms_{date_from}_{date_to}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON出力: {json_path}")

    # CSV出力
    csv_path = OUTPUT_DIR / f"{site_id}_ag_cv_search_terms_{date_from}_{date_to}.csv"
    export_csv(all_mapping, csv_path)

    # DB投入
    if not no_db:
        try:
            db_dir = OUTPUT_DIR / "db"
            db_path = db_dir / f"{site_id}.db"
            if db_path.exists():
                sys.path.insert(0, str(SCRIPT_DIR.parent / "google-ads"))
                # gads_db が利用不可の場合もあるので try
                try:
                    from gads_db import GadsDB, set_data_dir
                    set_data_dir(str(OUTPUT_DIR))
                    db = GadsDB(site_id)
                    db.init()
                    count = db.import_ag_cv_search_terms(output_data, save_raw=True)
                    print(f"  DB投入: {count} 行 → {db_path}")
                except ImportError:
                    print("  DB投入スキップ（gads_db モジュールが見つかりません）")
                except Exception as e:
                    print(f"  DB投入エラー: {e}")
            else:
                print(f"  DB投入スキップ（{db_path} が存在しません）")
        except Exception as e:
            print(f"  DB投入エラー: {e}")

    print(f"\n=== 完了: {len(all_mapping)} マッピング ===")
    return output_data


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Pmax AG別CV検索語句マッピング")
    parser.add_argument("--site", required=True, help="サイトID (例: 072)")
    parser.add_argument("--from", dest="date_from", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--campaign", help="特定のキャンペーンIDのみ")
    parser.add_argument("--no-db", action="store_true", help="DB投入をスキップ")
    args = parser.parse_args()

    run(args.site, args.date_from, args.date_to, args.campaign, args.no_db)


if __name__ == "__main__":
    main()
