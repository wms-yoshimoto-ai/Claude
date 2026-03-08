#!/usr/bin/env python3
"""
Google Ads データ取得スクリプト
認証情報・アカウント情報はすべて config/ フォルダのJSONファイルから読み込む

【使い方】
  # 全アカウントを取得
  python3 fetch_google_ads.py

  # 特定アカウントのみ取得
  python3 fetch_google_ads.py --account 3517620646

  # 取得期間を指定
  python3 fetch_google_ads.py --from 2026-02-01 --to 2026-02-28

【必要なもの】
  - Python 3.7 以上
  - requests ライブラリ（pip3 install requests）
  - config/credentials.json（認証情報）
  - config/accounts.json（アカウント一覧）
"""

import json
import sys
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# パス設定（すべてこのスクリプトからの相対パス）
# ============================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"
LOOKBACK_DAYS    = 7


# ============================================================
# 設定ファイルの読み込み
# ============================================================

def load_credentials():
    if not CREDENTIALS_FILE.exists():
        print(f"エラー: {CREDENTIALS_FILE} が見つかりません")
        sys.exit(1)
    with open(CREDENTIALS_FILE, "r") as f:
        data = json.load(f)
    return data["google_ads"]


def load_accounts(filter_query=None):
    """
    アカウント一覧を読み込む。
    filter_query にはサイトID・アカウント名・customer_id のいずれかを指定可能。
    例: "065" / "065 矯正 札幌" / "3517620646" / "351-762-0646"
    """
    if not ACCOUNTS_FILE.exists():
        print(f"エラー: {ACCOUNTS_FILE} が見つかりません")
        sys.exit(1)
    with open(ACCOUNTS_FILE, "r") as f:
        data = json.load(f)

    accounts = [a for a in data["accounts"] if a.get("active", True)]

    if filter_query:
        q = filter_query.strip()
        q_no_hyphen = q.replace("-", "")
        matched = [
            a for a in accounts
            if a.get("site_id")    == q
            or a.get("name")       == q
            or a.get("customer_id") == q
            or a["customer_id"].replace("-", "") == q_no_hyphen
        ]
        if not matched:
            print(f"エラー: '{filter_query}' に一致するアカウントが見つかりません")
            print("指定できる値: サイトID（例: 065）/ アカウント名（例: 065 矯正 札幌）/ customer_id")
            sys.exit(1)
        return matched

    return accounts


# ============================================================
# 認証
# ============================================================

def get_access_token(creds):
    res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     creds["oauth"]["client_id"],
            "client_secret": creds["oauth"]["client_secret"],
            "refresh_token": creds["oauth"]["refresh_token"],
            "grant_type":    "refresh_token",
        },
        timeout=30,
    )
    res.raise_for_status()
    return res.json()["access_token"]


# ============================================================
# Google Ads API
# ============================================================

def search_all(creds, access_token, customer_id, gaql):
    cid = customer_id.replace("-", "").replace(" ", "")
    url = f"https://googleads.googleapis.com/v22/customers/{cid}/googleAds:search"
    headers = {
        "Authorization":     f"Bearer {access_token}",
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
            raise Exception(f"API Error [{res.status_code}]: {res.text[:400]}")

        data = res.json()
        results.extend(data.get("results", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return results


def fetch_campaign_metrics(creds, access_token, customer_id, date_from, date_to):
    gaql = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """
    rows = search_all(creds, access_token, customer_id, gaql)

    campaigns = []
    for r in rows:
        c = r.get("campaign", {})
        m = r.get("metrics", {})
        cost   = int(m.get("costMicros", 0)) / 1_000_000
        conv   = float(m.get("conversions", 0))
        allc   = float(m.get("allConversions", 0))
        clicks = int(m.get("clicks", 0))
        imps   = int(m.get("impressions", 0))
        campaigns.append({
            "campaign_id":   c.get("id", ""),
            "campaign_name": c.get("name", ""),
            "status":        c.get("status", ""),
            "type":          c.get("advertisingChannelType", ""),
            "impressions":   imps,
            "clicks":        clicks,
            "cost_yen":      round(cost),
            "conversions":   round(conv, 2),
            "micro_conv":    round(allc - conv, 2),
            "ctr":           round(clicks / imps, 4) if imps > 0 else 0,
            "cpa":           round(cost / conv) if conv > 0 else None,
        })
    return campaigns


# ============================================================
# 保存
# ============================================================

def export_account(creds, access_token, account, date_from, date_to):
    print(f"  取得中: {account['name']} ({account['customer_id']}) / {date_from} 〜 {date_to}")

    campaigns = fetch_campaign_metrics(
        creds, access_token, account["customer_id"], date_from, date_to
    )

    totals = {
        "impressions": sum(c["impressions"] for c in campaigns),
        "clicks":      sum(c["clicks"]      for c in campaigns),
        "cost_yen":    sum(c["cost_yen"]    for c in campaigns),
        "conversions": round(sum(c["conversions"] for c in campaigns), 2),
        "micro_conv":  round(sum(c["micro_conv"]  for c in campaigns), 2),
    }
    totals["cpa"] = (
        round(totals["cost_yen"] / totals["conversions"])
        if totals["conversions"] > 0 else None
    )

    output = {
        "account_id":   account["customer_id"],
        "account_name": account["name"],
        "industry":     account.get("industry", ""),
        "url":          account.get("url", ""),
        "period":       {"from": date_from, "to": date_to},
        "fetched_at":   datetime.now().isoformat(),
        "totals":       totals,
        "campaigns":    campaigns,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / f"{account['customer_id']}_weekly.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ 保存: {filepath}")
    print(f"    費用: {totals['cost_yen']:,}円  CV: {totals['conversions']}  マイクロCV: {totals['micro_conv']}")
    return output


# ============================================================
# エントリーポイント
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads データ取得")
    parser.add_argument("--account", help="アカウントを指定（サイトID / アカウント名 / customer_id のいずれかで指定。省略時は全アカウント）")
    parser.add_argument("--from",    dest="date_from", help="取得開始日 YYYY-MM-DD")
    parser.add_argument("--to",      dest="date_to",   help="取得終了日 YYYY-MM-DD")
    args = parser.parse_args()

    today     = datetime.today()
    date_to   = args.date_to   or (today - timedelta(days=1)).strftime("%Y-%m-%d")
    date_from = args.date_from or (today - timedelta(days=LOOKBACK_DAYS + 1)).strftime("%Y-%m-%d")

    print("=" * 55)
    print("Google Ads データ取得スクリプト")
    print(f"実行日時 : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"取得期間 : {date_from} 〜 {date_to}")
    print("=" * 55)

    creds    = load_credentials()
    accounts = load_accounts(filter_query=args.account)
    print(f"対象アカウント数: {len(accounts)}")

    print("\nアクセストークン取得中...")
    try:
        access_token = get_access_token(creds)
        print("✓ 認証成功\n")
    except Exception as e:
        print(f"✗ 認証エラー: {e}")
        sys.exit(1)

    success, error = [], []
    for account in accounts:
        try:
            export_account(creds, access_token, account, date_from, date_to)
            success.append(account["name"])
        except Exception as e:
            print(f"  ✗ エラー: {e}")
            error.append(f"{account['name']}: {e}")

    print("\n" + "=" * 55)
    print(f"完了: 成功={len(success)}件  失敗={len(error)}件")
    print(f"保存先: {OUTPUT_DIR}")
    if error:
        print("エラー詳細:")
        for e in error:
            print(f"  - {e}")
    print("=" * 55)


if __name__ == "__main__":
    main()
