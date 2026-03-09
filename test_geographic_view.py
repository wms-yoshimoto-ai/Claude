#!/usr/bin/env python3
"""
geographic_view + geo_target_most_specific_location テスト
user_location_view との違いを検証する
"""

import json
import sys
import requests
from pathlib import Path

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

def main():
    creds = load_credentials()
    account = find_account("065")
    cid = account["customer_id"]
    token = get_access_token(creds)
    
    results = {}
    
    # テスト1: geographic_view + geo_target_most_specific_location
    print("=== Test 1: geographic_view + most_specific_location ===")
    gaql1 = """
        SELECT
            campaign.id, campaign.name,
            geographic_view.country_criterion_id,
            segments.geo_target_city,
            segments.geo_target_most_specific_location,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM geographic_view
        WHERE segments.date BETWEEN '2026-01-01' AND '2026-01-31'
        ORDER BY metrics.cost_micros DESC
    """
    rows1 = search_all(creds, token, cid, gaql1)
    if isinstance(rows1, dict) and "error" in rows1:
        print(f"  ERROR: {rows1}")
        results["test1"] = rows1
    else:
        total_imp = sum(int(r.get("metrics", {}).get("impressions", 0)) for r in rows1)
        total_clk = sum(int(r.get("metrics", {}).get("clicks", 0)) for r in rows1)
        print(f"  rows={len(rows1)} imp={total_imp} clk={total_clk}")
        results["test1"] = {
            "rows": len(rows1), "imp": total_imp, "clk": total_clk,
            "first_5": rows1[:5]
        }
    
    # テスト2: geographic_view + most_specific のみ (city なし)
    print("=== Test 2: geographic_view + most_specific only (no city) ===")
    gaql2 = """
        SELECT
            campaign.id, campaign.name,
            segments.geo_target_most_specific_location,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM geographic_view
        WHERE segments.date BETWEEN '2026-01-01' AND '2026-01-31'
        ORDER BY metrics.cost_micros DESC
    """
    rows2 = search_all(creds, token, cid, gaql2)
    if isinstance(rows2, dict) and "error" in rows2:
        print(f"  ERROR: {rows2}")
        results["test2"] = rows2
    else:
        total_imp = sum(int(r.get("metrics", {}).get("impressions", 0)) for r in rows2)
        total_clk = sum(int(r.get("metrics", {}).get("clicks", 0)) for r in rows2)
        print(f"  rows={len(rows2)} imp={total_imp} clk={total_clk}")
        results["test2"] = {
            "rows": len(rows2), "imp": total_imp, "clk": total_clk,
            "first_5": rows2[:5]
        }
    
    # テスト3: geographic_view + city のみ (baseline比較用)
    print("=== Test 3: geographic_view + city only (baseline) ===")
    gaql3 = """
        SELECT
            campaign.id, campaign.name,
            segments.geo_target_city,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM geographic_view
        WHERE segments.date BETWEEN '2026-01-01' AND '2026-01-31'
        ORDER BY metrics.cost_micros DESC
    """
    rows3 = search_all(creds, token, cid, gaql3)
    if isinstance(rows3, dict) and "error" in rows3:
        print(f"  ERROR: {rows3}")
        results["test3"] = rows3
    else:
        total_imp = sum(int(r.get("metrics", {}).get("impressions", 0)) for r in rows3)
        total_clk = sum(int(r.get("metrics", {}).get("clicks", 0)) for r in rows3)
        print(f"  rows={len(rows3)} imp={total_imp} clk={total_clk}")
        results["test3"] = {
            "rows": len(rows3), "imp": total_imp, "clk": total_clk,
            "first_5": rows3[:5]
        }

    # テスト4: user_location_view + most_specific (前回失敗したもの再試行)
    print("=== Test 4: user_location_view + most_specific_location ===")
    gaql4 = """
        SELECT
            campaign.id, campaign.name,
            segments.geo_target_city,
            segments.geo_target_most_specific_location,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM user_location_view
        WHERE segments.date BETWEEN '2026-01-01' AND '2026-01-31'
        ORDER BY metrics.cost_micros DESC
    """
    rows4 = search_all(creds, token, cid, gaql4)
    if isinstance(rows4, dict) and "error" in rows4:
        print(f"  ERROR: {rows4}")
        results["test4"] = rows4
    else:
        total_imp = sum(int(r.get("metrics", {}).get("impressions", 0)) for r in rows4)
        total_clk = sum(int(r.get("metrics", {}).get("clicks", 0)) for r in rows4)
        print(f"  rows={len(rows4)} imp={total_imp} clk={total_clk}")
        results["test4"] = {
            "rows": len(rows4), "imp": total_imp, "clk": total_clk,
            "first_5": rows4[:5]
        }
    
    # 結果保存
    out_path = OUTPUT_DIR / "065_geographic_view_test.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {out_path}")

if __name__ == "__main__":
    main()
