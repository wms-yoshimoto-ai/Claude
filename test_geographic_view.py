#!/usr/bin/env python3
"""
geographic_view + geo_target_most_specific_location テスト v2
全行保存 + geoTargetConstants 名前解決
"""

import json
import sys
import requests
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR = Path.home() / "Documents" / "GoogleAds_Data"

def load_credentials():
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)["google_ads"]

def find_account(query):
    with open(ACCOUNTS_FILE) as f:
        data = json.load(f)
    for a in data["accounts"]:
        if a.get("site_id") == query:
            return a
    return None

def get_access_token(creds):
    res = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": creds["oauth"]["client_id"],
        "client_secret": creds["oauth"]["client_secret"],
        "refresh_token": creds["oauth"]["refresh_token"],
        "grant_type": "refresh_token",
    }, timeout=30)
    res.raise_for_status()
    return res.json()["access_token"]

def search_all(creds, token, customer_id, gaql):
    cid = customer_id.replace("-", "")
    url = f"https://googleads.googleapis.com/v22/customers/{cid}/googleAds:search"
    headers = {
        "Authorization": f"Bearer {token}",
        "developer-token": creds["developer_token"],
        "login-customer-id": creds["mcc_customer_id"],
        "Content-Type": "application/json",
    }
    results, page_token = [], None
    while True:
        payload = {"query": gaql}
        if page_token:
            payload["pageToken"] = page_token
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        if res.status_code != 200:
            return {"error": res.status_code, "message": res.text[:500]}
        data = res.json()
        results.extend(data.get("results", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results

def resolve_geo_names(creds, token, geo_ids):
    """geoTargetConstants:suggest で日本語名を取得"""
    url = "https://googleads.googleapis.com/v22/geoTargetConstants:suggest"
    headers = {
        "Authorization": f"Bearer {token}",
        "developer-token": creds["developer_token"],
        "Content-Type": "application/json",
    }
    result = {}
    valid_ids = [g for g in geo_ids if g and "geoTargetConstants/" in g]
    batch_size = 50
    for i in range(0, len(valid_ids), batch_size):
        batch = valid_ids[i:i+batch_size]
        payload = {
            "locale": "ja",
            "geoTargets": {"geoTargetConstants": batch},
        }
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=30)
            if res.status_code != 200:
                print(f"  [warn] suggest error [{res.status_code}]: {res.text[:200]}")
                continue
            data = res.json()
            for item in data.get("geoTargetConstantSuggestions", []):
                gc = item.get("geoTargetConstant", {})
                raw_id = str(gc.get("id", ""))
                key = f"geoTargetConstants/{raw_id}"
                result[key] = {
                    "name": gc.get("name", ""),
                    "canonical_name": gc.get("canonicalName", ""),
                    "target_type": gc.get("targetType", ""),
                }
        except Exception as e:
            print(f"  [warn] suggest exception: {e}")
    return result

def main():
    creds = load_credentials()
    account = find_account("065")
    cid = account["customer_id"]
    token = get_access_token(creds)

    # geographic_view + most_specific + city + campaign
    print("=== geographic_view + most_specific + city ===")
    gaql = """
        SELECT
            campaign.id, campaign.name,
            geographic_view.country_criterion_id,
            segments.geo_target_city,
            segments.geo_target_most_specific_location,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.all_conversions
        FROM geographic_view
        WHERE segments.date BETWEEN '2026-01-01' AND '2026-01-31'
        ORDER BY segments.date ASC, metrics.cost_micros DESC
    """
    rows = search_all(creds, token, cid, gaql)
    if isinstance(rows, dict) and "error" in rows:
        print(f"ERROR: {rows}")
        return

    total_imp = sum(int(r.get("metrics", {}).get("impressions", 0)) for r in rows)
    total_clk = sum(int(r.get("metrics", {}).get("clicks", 0)) for r in rows)
    total_cost = sum(int(r.get("metrics", {}).get("costMicros", 0)) for r in rows) / 1_000_000
    print(f"rows={len(rows)} imp={total_imp} clk={total_clk} cost=¥{total_cost:,.0f}")

    # ユニーク geo ID 収集 → 名前解決
    geo_ids = set()
    for r in rows:
        seg = r.get("segments", {})
        geo_ids.add(seg.get("geoTargetCity", ""))
        geo_ids.add(seg.get("geoTargetMostSpecificLocation", ""))
    geo_ids.discard("")

    print(f"\nユニーク geo ID: {len(geo_ids)}件")
    print("名前解決中...")
    geo_map = resolve_geo_names(creds, token, geo_ids)
    print(f"解決済み: {len(geo_map)}件")

    # target_type 分布
    type_count = defaultdict(int)
    for v in geo_map.values():
        type_count[v.get("target_type", "?")] += 1
    print(f"target_type 分布: {dict(type_count)}")

    # most_specific のうち区レベルを確認
    print("\n=== most_specific の名前解決結果（サンプル）===")
    ms_ids = set(r.get("segments", {}).get("geoTargetMostSpecificLocation", "") for r in rows)
    ms_ids.discard("")
    for ms_id in sorted(ms_ids)[:30]:
        info = geo_map.get(ms_id, {})
        name = info.get("name", "?")
        ttype = info.get("target_type", "?")
        canon = info.get("canonical_name", "?")
        print(f"  {ms_id} → {name} ({ttype}) [{canon}]")

    # 結果保存
    out_path = OUTPUT_DIR / "065_geographic_view_test.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "summary": {
                "rows": len(rows), "imp": total_imp, "clk": total_clk,
                "cost": total_cost,
            },
            "geo_map": geo_map,
            "all_rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {out_path}")

if __name__ == "__main__":
    main()
