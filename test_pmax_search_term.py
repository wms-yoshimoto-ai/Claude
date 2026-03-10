#!/usr/bin/env python3
"""Pmax検索語句のAPI取得テスト"""
import json, sys, requests
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
with open(SCRIPT_DIR / 'config/credentials.json') as f:
    creds = json.load(f)['google_ads']
with open(SCRIPT_DIR / 'config/accounts.json') as f:
    accounts = json.load(f)['accounts']

acct = next(a for a in accounts if a['site_id'] == '072')
cid = acct['customer_id'].replace('-','')

oauth = creds['oauth']
res = requests.post('https://oauth2.googleapis.com/token', data={
    'client_id': oauth['client_id'], 'client_secret': oauth['client_secret'],
    'refresh_token': oauth['refresh_token'], 'grant_type': 'refresh_token'}, timeout=30)
token = res.json()['access_token']

url = f'https://googleads.googleapis.com/v22/customers/{cid}/googleAds:search'
headers = {
    'Authorization': f'Bearer {token}',
    'developer-token': creds['developer_token'],
    'login-customer-id': creds['mcc_customer_id'],
    'Content-Type': 'application/json',
}

results = {}

# Test 1: search_term_view WITHOUT ad_group
print("=== Test 1: search_term_view (ad_groupなし) ===")
gaql1 = """
    SELECT
        search_term_view.search_term,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions
    FROM search_term_view
    WHERE segments.date BETWEEN '2026-02-01' AND '2026-02-28'
      AND campaign.status != 'REMOVED'
      AND campaign.id = 23367869010
    ORDER BY metrics.impressions DESC
    LIMIT 20
"""
res1 = requests.post(url, headers=headers, json={'query': gaql1}, timeout=60)
print(f"Status: {res1.status_code}")
r1 = res1.json()
results['test1_search_term_view'] = {
    'status': res1.status_code,
    'count': len(r1.get('results', [])),
    'error': r1.get('error', {}).get('message', '') if res1.status_code != 200 else '',
    'sample': []
}
for r in r1.get('results', [])[:10]:
    st = r.get('searchTermView', {}).get('searchTerm', '?')
    m = r.get('metrics', {})
    entry = {'term': st, 'imp': m.get('impressions', 0), 'clk': m.get('clicks', 0)}
    results['test1_search_term_view']['sample'].append(entry)
    print(f"  {st} | imp={entry['imp']} clk={entry['clk']}")
if not r1.get('results'):
    print(f"  データなし" + (f" ({r1.get('error', {}).get('message', '')[:200]})" if res1.status_code != 200 else ""))

# Test 2: campaign_search_term_insight
print("\n=== Test 2: campaign_search_term_insight ===")
gaql2 = """
    SELECT
        campaign_search_term_insight.category_label,
        campaign_search_term_insight.id,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions
    FROM campaign_search_term_insight
    WHERE segments.date BETWEEN '2026-02-01' AND '2026-02-28'
      AND campaign.id = 23367869010
    ORDER BY metrics.impressions DESC
    LIMIT 20
"""
res2 = requests.post(url, headers=headers, json={'query': gaql2}, timeout=60)
print(f"Status: {res2.status_code}")
r2 = res2.json()
results['test2_search_term_insight'] = {
    'status': res2.status_code,
    'count': len(r2.get('results', [])),
    'error': r2.get('error', {}).get('message', '') if res2.status_code != 200 else '',
    'sample': []
}
for r in r2.get('results', [])[:10]:
    label = r.get('campaignSearchTermInsight', {}).get('categoryLabel', '?')
    m = r.get('metrics', {})
    entry = {'label': label, 'imp': m.get('impressions', 0), 'clk': m.get('clicks', 0)}
    results['test2_search_term_insight']['sample'].append(entry)
    print(f"  {label} | imp={entry['imp']} clk={entry['clk']}")
if not r2.get('results'):
    print(f"  データなし" + (f" ({r2.get('error', {}).get('message', '')[:200]})" if res2.status_code != 200 else ""))

# Test 3: search_term_view WITH ad_group (現行スクリプトと同じ)
print("\n=== Test 3: search_term_view (ad_groupあり=現行) ===")
gaql3 = """
    SELECT
        search_term_view.search_term,
        campaign.name,
        ad_group.name,
        metrics.impressions,
        metrics.clicks
    FROM search_term_view
    WHERE segments.date BETWEEN '2026-02-01' AND '2026-02-28'
      AND campaign.status != 'REMOVED'
      AND ad_group.status != 'REMOVED'
      AND campaign.id = 23367869010
    ORDER BY metrics.impressions DESC
    LIMIT 20
"""
res3 = requests.post(url, headers=headers, json={'query': gaql3}, timeout=60)
print(f"Status: {res3.status_code}")
r3 = res3.json()
results['test3_with_ad_group'] = {
    'status': res3.status_code,
    'count': len(r3.get('results', [])),
    'error': r3.get('error', {}).get('message', '') if res3.status_code != 200 else ''
}
if r3.get('results'):
    print(f"  {len(r3['results'])} 件")
else:
    print(f"  データなし" + (f" ({r3.get('error', {}).get('message', '')[:200]})" if res3.status_code != 200 else ""))

# 結果をJSONに保存
out_path = Path.home() / 'Documents/GoogleAds_Data/072_pmax_search_term_test.json'
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"\n結果保存: {out_path}")
