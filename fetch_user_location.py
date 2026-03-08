#!/usr/bin/env python3
"""
ユーザーの所在地レポート 取得スクリプト
管理画面の「市区町村別（ユーザーの所在地）」と同じデータをAPIで取得し、
CSVと照合して一致を確認する

【使い方】
  python3 fetch_user_location.py --site 065 --from 2026-01-01 --to 2026-02-28
  python3 fetch_user_location.py --site 065 --campaign 23335569301 --from 2026-01-01 --to 2026-02-28

【照合用CSVの指定】
  python3 fetch_user_location.py --site 065 --from 2026-01-01 --to 2026-02-28 --csv path/to/file.csv
"""

import json
import sys
import csv
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

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
# Google Ads API
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

def get_campaigns(creds, token, customer_id):
    """キャンペーン一覧を取得（IDと名前のマッピング用）"""
    gaql = """
        SELECT campaign.id, campaign.name
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    rows = search_all(creds, token, customer_id, gaql)
    return {str(r["campaign"]["id"]): r["campaign"]["name"] for r in rows}

def fetch_user_location(creds, token, customer_id, campaign_id, date_from, date_to, debug_path=None):
    """
    ユーザーの所在地データを取得（user_location_view）
    - targeting_location=FALSE が「ユーザーの所在地」（物理的な所在地）
    - targeting_location=TRUE  が「対象地域」（ターゲティング設定）
    - campaign.id フィルタはWHERE句では効かないため、Python側でフィルタする
    - debug_path を指定すると、フィルタ前の全生データをJSONに保存する
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
        ORDER BY metrics.cost_micros DESC
    """
    rows = search_all(creds, token, customer_id, gaql)

    # ── デバッグ: 生の全行を保存 ──────────────────────────────
    if debug_path:
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump({
                "total_rows": len(rows),
                "first_row_keys": list(rows[0].keys()) if rows else [],
                "first_row_sample": rows[0] if rows else {},
                "all_rows": rows,
            }, f, ensure_ascii=False, indent=2)
        print(f"  [DEBUG] 生データ保存: {debug_path} ({len(rows)}件)")
        if rows:
            print(f"  [DEBUG] 1件目のキー: {list(rows[0].keys())}")
            print(f"  [DEBUG] 1件目の内容:")
            print(json.dumps(rows[0], ensure_ascii=False, indent=4))

    # targeting_location の内訳を確認（キー名をすべてのパターンで試す）
    def get_targeting_location(r):
        # camelCase (Google Ads API の標準レスポンス形式)
        v = r.get("userLocationView", {}).get("targetingLocation")
        if v is not None:
            return v
        # snake_case の場合も念のため確認
        v = r.get("user_location_view", {}).get("targeting_location")
        if v is not None:
            return v
        return None  # キーが存在しない

    tl_true  = [r for r in rows if get_targeting_location(r) is True]
    tl_false = [r for r in rows if get_targeting_location(r) is False]
    tl_none  = [r for r in rows if get_targeting_location(r) is None]
    print(f"  [内訳] targeting_location=TRUE: {len(tl_true)}件 / FALSE: {len(tl_false)}件 / キーなし: {len(tl_none)}件 / 全体: {len(rows)}件")

    # 「ユーザーの所在地」= TRUE（ターゲット地域に物理的にいたユーザー）＋
    #                       FALSE（ターゲット外からも広告を見たユーザー）の両方を使用
    # ※ targetingLocation=TRUE がターゲット設定地域（例:札幌）にいたユーザーの実際の所在地データ
    # ※ targetingLocation=FALSE はターゲット外地域にいたユーザーの所在地データ
    # ※ Google広告UIの「ユーザーの所在地」レポートはTRUE+FALSEの合計値
    user_loc_rows = rows  # フィルタなし：全データを使用

    # キャンペーンIDでPython側フィルタ
    if campaign_id:
        before = len(user_loc_rows)
        user_loc_rows = [r for r in user_loc_rows if str(r.get("campaign", {}).get("id", "")) == str(campaign_id)]
        print(f"  [フィルタ後] campaign_id={campaign_id}: {before}件 → {len(user_loc_rows)}件")

    return user_loc_rows


# ============================================================
# CSV読み込み・集計
# ============================================================
def load_csv_totals(csv_path):
    """CSVから合計値を読み込む"""
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
        "imp":   sum(to_f(r[3]) for r in rows),
        "clk":   sum(to_f(r[4]) for r in rows),
        "cost":  sum(to_f(r[6]) for r in rows),
        "conv":  sum(to_f(r[7]) for r in rows),
        "allcv": sum(to_f(r[8]) for r in rows),
    }


# ============================================================
# メイン
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="ユーザーの所在地データ取得・CSV照合")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--campaign", help="キャンペーンID（省略時は全キャンペーン）")
    parser.add_argument("--from",     dest="date_from", required=True, help="開始日 YYYY-MM-DD")
    parser.add_argument("--to",       dest="date_to",   required=True, help="終了日 YYYY-MM-DD")
    parser.add_argument("--csv",      help="照合するCSVファイルのパス（省略可）")
    parser.add_argument("--debug",    action="store_true", help="生のAPIレスポンスをJSONに保存してデバッグ")
    args = parser.parse_args()

    print("=" * 60)
    print("ユーザーの所在地レポート 取得・照合スクリプト")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    creds   = load_credentials()
    account = find_account(args.site)
    cid     = account["customer_id"]
    print(f"対象アカウント: {account['site_id']} {account['name']} ({cid})")
    print(f"取得期間: {args.date_from} 〜 {args.date_to}")

    # アクセストークン取得
    print("\nアクセストークン取得中...")
    token = get_access_token(creds)
    print("✓ 認証成功")

    # キャンペーン一覧取得（IDと名前のマッピング）
    campaigns = get_campaigns(creds, token, cid)
    if args.campaign:
        camp_name = campaigns.get(args.campaign, "不明")
        print(f"対象キャンペーン: {camp_name} (ID: {args.campaign})")
    else:
        # キャンペーン名から自動選択（065 矯正 all の検索）
        all_camp = {k: v for k, v in campaigns.items() if "all" in v.lower() or "矯正" in v}
        if all_camp:
            print(f"利用可能なキャンペーン:")
            for k, v in campaigns.items():
                print(f"  ID:{k} → {v}")

    # データ取得
    print("\nユーザーの所在地データ取得中...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    debug_path = None
    if args.debug:
        debug_path = OUTPUT_DIR / f"{account['site_id']}_user_location_debug_raw.json"
        print(f"  [DEBUG モード] 生データ保存先: {debug_path}")
    rows = fetch_user_location(creds, token, cid, args.campaign, args.date_from, args.date_to, debug_path=debug_path)
    print(f"✓ {len(rows)} 件取得")

    # API結果の集計
    api = {"rows": len(rows), "imp": 0, "clk": 0, "cost": 0.0, "conv": 0.0, "allcv": 0.0}
    for r in rows:
        m = r.get("metrics", {})
        api["imp"]   += int(m.get("impressions", 0))
        api["clk"]   += int(m.get("clicks", 0))
        api["cost"]  += int(m.get("costMicros", 0)) / 1_000_000
        api["conv"]  += float(m.get("conversions", 0))
        api["allcv"] += float(m.get("allConversions", 0))

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
            "account":   account,
            "period":    {"from": args.date_from, "to": args.date_to},
            "campaign_id": args.campaign,
            "fetched_at": datetime.now().isoformat(),
            "api_totals": api,
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {out_path}")

    # CSV照合
    if args.csv:
        print(f"\n【CSV照合】{args.csv}")
        csved = load_csv_totals(args.csv)
        print(f"{'項目':<16} {'CSV':>12} {'API':>12} {'一致':>6}")
        print("-" * 50)
        checks = [
            ("表示回数",       csved["imp"],   api["imp"]),
            ("クリック数",     csved["clk"],   api["clk"]),
            ("費用(円)",       csved["cost"],  api["cost"]),
            ("コンバージョン", csved["conv"],  api["conv"]),
            ("全コンバージョン",csved["allcv"],api["allcv"]),
        ]
        all_ok = True
        for label, cv, av in checks:
            ok = abs(cv - av) < 1.0  # 費用はマイクロ円変換の端数誤差（±1円）を許容
            mark = "✓" if ok else "✗"
            if not ok: all_ok = False
            print(f"{label:<16} {cv:>12,.1f} {av:>12,.1f} {mark:>6}")
        print("-" * 50)
        print("結果:", "✓ 全項目一致" if all_ok else "✗ 不一致あり")

    print("\n完了")

if __name__ == "__main__":
    main()
