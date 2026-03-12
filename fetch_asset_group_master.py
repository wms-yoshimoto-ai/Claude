#!/usr/bin/env python3
"""
アセットグループ マスター取得スクリプト

P-MAXキャンペーンのアセットグループID・名前・ステータスを取得し、
asset_group_master テーブルに投入する。

campaigns.json にはAGレベルの情報がないため、このスクリプトで
全P-MAXサイトのAG一覧を一括取得してDBに登録する。

【使い方 — Mac上で直接実行】
  # 特定サイトのAGマスター取得
  python3 fetch_asset_group_master.py --site 065

  # 全P-MAXサイトのAGマスター一括取得
  python3 fetch_asset_group_master.py --all

  # AGマスターが未登録のサイトのみ取得
  python3 fetch_asset_group_master.py --missing-only

【使い方 — Cowork VM から trigger_fetch 経由】
  from trigger_fetch import fetch_and_read
  data = fetch_and_read(site="065", date_from="2026-01-01", date_to="2026-01-31",
                        action="fetch_asset_group_master")

  # DBに投入
  from gads_db import GadsDB, set_data_dir
  db = GadsDB("065")
  db.init()
  count = db.import_asset_group_master(data)

【出力JSON】
  {
    "site_id": "065",
    "customer_id": "xxx-xxx-xxxx",
    "fetched_at": "2026-03-12T10:00:00",
    "total_campaigns": 1,
    "total_asset_groups": 3,
    "asset_groups": [
      {
        "campaign_id": "23335615195",
        "campaign_name": "065 矯正 Pmax（札幌市）",
        "asset_group_id": "12345678",
        "asset_group_name": "AG名",
        "status": "ENABLED",
        "final_url": "https://..."
      }
    ]
  }
"""

import json
import sys
import argparse
import requests
from datetime import datetime
from pathlib import Path

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
CAMPAIGNS_FILE   = SCRIPT_DIR / "config" / "campaigns.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"


# ============================================================
# 設定ファイル読み込み
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


def get_pmax_site_ids() -> list[str]:
    """campaigns.json から P-MAX キャンペーンを持つサイトID一覧を取得"""
    if not CAMPAIGNS_FILE.exists():
        print(f"エラー: {CAMPAIGNS_FILE} が見つかりません")
        sys.exit(1)
    with open(CAMPAIGNS_FILE) as f:
        raw = json.load(f)
    # campaigns.json は {"campaigns": [...]} 形式
    campaigns = raw["campaigns"] if isinstance(raw, dict) else raw
    pmax_sites = set()
    for c in campaigns:
        if c.get("campaign_type") == "P-MAX":
            pmax_sites.add(c["site_id"])
    return sorted(pmax_sites)


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
# Google Ads API
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
# AGマスター取得
# ============================================================

def fetch_ag_master(creds: dict, token: str, customer_id: str) -> list:
    """
    asset_group リソースからAGマスター情報を取得（メトリクスなし）。
    日付条件不要 — 現在のAG設定をスナップショットとして取得。
    """
    gaql = """
        SELECT
            asset_group.id,
            asset_group.name,
            asset_group.status,
            asset_group.final_urls,
            campaign.id,
            campaign.name
        FROM asset_group
        WHERE campaign.status != 'REMOVED'
        ORDER BY campaign.name, asset_group.name
    """
    return search_all(creds, token, customer_id, gaql)


def parse_ag_results(api_results: list) -> list[dict]:
    """APIレスポンスをフラットなdict配列に変換"""
    asset_groups = []
    for r in api_results:
        ag = r.get("assetGroup", {})
        cmp = r.get("campaign", {})

        final_urls = ag.get("finalUrls", [])
        final_url = final_urls[0] if final_urls else ""

        asset_groups.append({
            "campaign_id": str(cmp.get("id", "")),
            "campaign_name": cmp.get("name", ""),
            "asset_group_id": str(ag.get("id", "")),
            "asset_group_name": ag.get("name", ""),
            "status": ag.get("status", ""),
            "final_url": final_url,
        })
    return asset_groups


def fetch_site_ag_master(site_id: str, creds: dict = None, token: str = None) -> dict:
    """指定サイトのAGマスターを取得してJSON構造で返す"""
    if creds is None:
        creds = load_credentials()
    if token is None:
        token = get_access_token(creds)

    account = load_account(site_id)
    cid = account["customer_id"]

    print(f"  [{site_id}] {account['name']} ... ", end="", flush=True)

    try:
        raw = fetch_ag_master(creds, token, cid)
        asset_groups = parse_ag_results(raw)

        # キャンペーン数
        campaign_ids = set(ag["campaign_id"] for ag in asset_groups)

        result = {
            "site_id": site_id,
            "customer_id": cid,
            "account_name": account.get("name", ""),
            "fetched_at": datetime.now().isoformat(),
            "total_campaigns": len(campaign_ids),
            "total_asset_groups": len(asset_groups),
            "asset_groups": asset_groups,
        }
        print(f"{len(asset_groups)} AG ({len(campaign_ids)} campaigns)")
        return result

    except Exception as e:
        print(f"エラー: {e}")
        return {
            "site_id": site_id,
            "customer_id": cid,
            "account_name": account.get("name", ""),
            "fetched_at": datetime.now().isoformat(),
            "error": str(e),
            "total_campaigns": 0,
            "total_asset_groups": 0,
            "asset_groups": [],
        }


# ============================================================
# DB投入ヘルパー
# ============================================================

def import_to_db(site_id: str, data: dict, data_dir: str = None):
    """AGマスターデータをDBに投入"""
    # gads_db は google-ads スキルディレクトリにあるため、動的にimport
    try:
        from gads_db import GadsDB, set_data_dir
    except ImportError:
        # スクリプト単体実行時のフォールバック
        import glob
        skill_dirs = glob.glob(str(Path.home() / "Desktop/Claude/google-ads"))
        if skill_dirs:
            sys.path.insert(0, skill_dirs[0])
        from gads_db import GadsDB, set_data_dir

    if data_dir:
        set_data_dir(data_dir)

    db = GadsDB(site_id)
    db.init()
    count = db.import_asset_group_master(data)
    db.close()
    return count


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="P-MAX アセットグループ マスター取得")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--site", help="特定サイトIDのAGマスターを取得")
    group.add_argument("--all", action="store_true", help="全P-MAXサイトのAGマスターを一括取得")
    group.add_argument("--missing-only", action="store_true",
                       help="asset_group_master テーブルが空のサイトのみ取得")
    parser.add_argument("--no-db", action="store_true", help="DB投入をスキップ（JSONのみ保存）")
    args = parser.parse_args()

    creds = load_credentials()
    token = get_access_token(creds)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.site:
        sites = [args.site]
    else:
        sites = get_pmax_site_ids()
        print(f"P-MAXサイト: {len(sites)} 件")

    if args.missing_only:
        # DBにAGマスターが登録済みのサイトをスキップ
        try:
            from gads_db import GadsDB, set_data_dir
        except ImportError:
            import glob
            skill_dirs = glob.glob(str(Path.home() / "Desktop/Claude/google-ads"))
            if skill_dirs:
                sys.path.insert(0, skill_dirs[0])
            from gads_db import GadsDB, set_data_dir

        set_data_dir(str(OUTPUT_DIR))
        skip = []
        for sid in sites:
            try:
                db = GadsDB(sid)
                db.init()
                ags = db.get_asset_groups()
                db.close()
                if len(ags) > 0:
                    skip.append(sid)
            except Exception:
                pass
        sites = [s for s in sites if s not in skip]
        print(f"未登録サイト: {len(sites)} 件（登録済み {len(skip)} 件スキップ）")

    # 全サイト処理
    summary = []
    for site_id in sites:
        result = fetch_site_ag_master(site_id, creds, token)

        # JSON保存
        json_out = OUTPUT_DIR / f"{site_id}_asset_group_master.json"
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # DB投入
        if not args.no_db and result.get("asset_groups"):
            try:
                count = import_to_db(site_id, result, str(OUTPUT_DIR))
                result["db_imported"] = count
            except Exception as e:
                print(f"    ⚠ DB投入エラー: {e}")
                result["db_imported"] = 0

        summary.append({
            "site_id": site_id,
            "ag_count": result.get("total_asset_groups", 0),
            "campaigns": result.get("total_campaigns", 0),
            "error": result.get("error"),
        })

    # サマリー
    print(f"\n{'='*50}")
    print(f"完了: {len(summary)} サイト")
    total_ag = sum(s["ag_count"] for s in summary)
    errors = [s for s in summary if s.get("error")]
    print(f"合計AG数: {total_ag}")
    if errors:
        print(f"エラー: {len(errors)} 件")
        for e in errors:
            print(f"  {e['site_id']}: {e['error'][:80]}")


if __name__ == "__main__":
    main()
