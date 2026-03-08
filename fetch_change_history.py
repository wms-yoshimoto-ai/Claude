#!/usr/bin/env python3
"""
Google Ads 変更履歴取得スクリプト
管理画面の「変更履歴レポート」に相当するデータを change_event リソースから取得する

【重要な制限事項】
  - Google Ads API の change_event は直近30日間のデータのみ保持
    （管理画面では最大2年分を表示できるが、APIでは取得不可）
  - 「変更内容」列のテキストは管理画面とは異なる表現になる
    （APIは構造化データを返すため、UI生成文字列は再現できない）

【使い方】
  python3 fetch_change_history.py --site 065
  python3 fetch_change_history.py --site 065 --campaign 23335615195
  python3 fetch_change_history.py --site 065 --from 2026-02-01 --to 2026-03-08

【出力列】
  日時, ユーザー, キャンペーン, 広告グループ, 変更内容
"""

import json
import sys
import csv
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

CSV_COLUMNS = [
    "日時",
    "ユーザー",
    "キャンペーン",
    "広告グループ",
    "変更内容",
]

# ── リソース種別マッピング ────────────────────────────────────

RESOURCE_TYPE_MAP = {
    "CAMPAIGN":                         "キャンペーン",
    "AD_GROUP":                         "広告グループ",
    "AD_GROUP_AD":                      "広告",
    "AD_GROUP_CRITERION":               "広告グループのターゲット",
    "CAMPAIGN_CRITERION":               "キャンペーンのターゲット",
    "CAMPAIGN_BUDGET":                  "予算",
    "ASSET":                            "アセット",
    "ASSET_GROUP":                      "アセットグループ",
    "ASSET_GROUP_ASSET":                "アセットグループのアセット",
    "ASSET_GROUP_SIGNAL":               "オーディエンスシグナル",
    "ASSET_GROUP_LISTING_GROUP_FILTER": "リスティンググループ",
    "CAMPAIGN_ASSET":                   "キャンペーンアセット",
    "AD_GROUP_ASSET":                   "広告グループのアセット",
    "CUSTOMER_ASSET":                   "顧客アセット",
    "CAMPAIGN_ASSET_SET":               "キャンペーンアセットセット",
    "ASSET_SET_ASSET":                  "アセットセット",
    "SMART_CAMPAIGN_SETTING":           "スマートキャンペーン設定",
    "FEED":                             "フィード",
    "FEED_ITEM":                        "フィードアイテム",
    "EXTENSION_FEED_ITEM":              "広告表示オプション",
    "CUSTOMER":                         "アカウント",
    "UNKNOWN":                          "不明",
    "UNSPECIFIED":                      "未設定",
}

OPERATION_MAP = {
    "CREATE":      "作成",
    "UPDATE":      "変更",
    "REMOVE":      "削除",
    "UNSPECIFIED": "操作不明",
    "UNKNOWN":     "操作不明",
}

# ── フィールド名 → 日本語マッピング ──────────────────────────

FIELD_NAME_MAP = {
    # キャンペーン
    "campaign.status":                                      "ステータス",
    "campaign.name":                                        "名前",
    "campaign.start_date":                                  "開始日",
    "campaign.end_date":                                    "終了日",
    "campaign.bidding_strategy_type":                       "入札戦略",
    "campaign.maximize_conversions.target_cpa_micros":      "目標CPA",
    "campaign.maximize_conversion_value.target_roas":       "目標ROAS",
    "campaign.target_cpa.target_cpa_micros":                "目標CPA",
    "campaign.geo_target_type_setting.positive_geo_target_type": "地域ターゲット設定",
    "campaign.advertising_channel_type":                    "キャンペーンタイプ",
    "campaign.optimization_goal_setting.optimization_goal_types": "最適化目標",
    # 予算
    "campaign_budget.amount_micros":                        "予算額",
    "campaign_budget.delivery_method":                      "配信方法",
    # 広告グループ
    "ad_group.status":                                      "ステータス",
    "ad_group.name":                                        "名前",
    "ad_group.cpc_bid_micros":                              "入札単価（CPC）",
    "ad_group.cpm_bid_micros":                              "入札単価（CPM）",
    # 広告
    "ad_group_ad.status":                                   "広告ステータス",
    "ad_group_ad.ad.final_urls":                            "最終ページURL",
    "ad_group_ad.ad.responsive_search_ad.headlines":        "広告見出し",
    "ad_group_ad.ad.responsive_search_ad.descriptions":     "説明文",
    # ターゲット
    "campaign_criterion.negative":                          "除外設定",
    "campaign_criterion.location.geo_target_constant":      "地域ターゲット",
    "campaign_criterion.proximity.radius":                  "近隣地域の半径",
    "campaign_criterion.language.language_constant":        "言語",
    "ad_group_criterion.keyword.text":                      "キーワード",
    "ad_group_criterion.keyword.match_type":                "マッチタイプ",
    "ad_group_criterion.negative":                          "除外設定",
    # アセットグループ
    "asset_group.status":                                   "ステータス",
    "asset_group.name":                                     "名前",
    "asset_group.final_urls":                               "最終ページURL",
    "asset_group.path1":                                    "パス1",
    "asset_group.path2":                                    "パス2",
}


def field_to_ja(field: str) -> str:
    """フィールドパス → 日本語名（未定義の場合は末尾のフィールド名）"""
    if field in FIELD_NAME_MAP:
        return FIELD_NAME_MAP[field]
    # 未定義: パスの最後の部分を返す
    parts = field.split(".")
    return parts[-1]


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
# change_event 取得
# ============================================================

def fetch_change_events(creds, token, customer_id,
                        date_from: str, date_to: str,
                        campaign_id=None) -> list:
    """
    change_event リソースから変更履歴を取得する。

    注意: change_event の日時フィルタは 'YYYY-MM-DD HH:MM:SS' 形式。
          API 側のデータ保持は直近30日のみ。
    """
    # 日付 → datetime 文字列に変換
    dt_from = f"{date_from} 00:00:00"
    dt_to   = f"{date_to} 23:59:59"

    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""

    gaql = f"""
        SELECT
            change_event.change_date_time,
            change_event.user_email,
            change_event.change_resource_type,
            change_event.resource_change_operation,
            change_event.changed_fields,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name
        FROM change_event
        WHERE change_event.change_date_time >= '{dt_from}'
          AND change_event.change_date_time <= '{dt_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        LIMIT 10000
    """
    return search_all(creds, token, customer_id, gaql)


# ============================================================
# 1行変換
# ============================================================

def build_change_content(resource_type: str, operation: str,
                         changed_fields: str) -> str:
    """
    resource_type + operation + changed_fields から「変更内容」列テキストを生成する。
    管理画面の文字列とは異なるが、変更の種類・対象フィールドを伝える。
    """
    rtype_ja = RESOURCE_TYPE_MAP.get(resource_type, resource_type)
    op_ja    = OPERATION_MAP.get(operation, operation)

    # changed_fields はカンマ区切りのフィールドパス文字列
    if changed_fields:
        fields = [f.strip() for f in changed_fields.split(",") if f.strip()]
        fields_ja = [field_to_ja(f) for f in fields]
        # 重複排除・整形
        seen, unique_fields = set(), []
        for fj in fields_ja:
            if fj not in seen:
                seen.add(fj)
                unique_fields.append(fj)
        fields_str = "、".join(unique_fields)
        content = f"{rtype_ja}を{op_ja}しました\n  変更フィールド: {fields_str}"
    else:
        content = f"{rtype_ja}を{op_ja}しました"

    return content


def row_to_csv(r: dict) -> dict:
    ce          = r.get("changeEvent", {})
    campaign    = r.get("campaign", {})
    ad_group    = r.get("adGroup", {})

    # 日時: '2026-03-01T11:34:53+00:00' → '2026/03/01 20:34:53' (JST +9h)
    dt_str = ce.get("changeDateTime", "")
    if dt_str:
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            # UTC → JST
            dt_jst = dt.astimezone(tz=None)  # ローカルタイムゾーン（通常 JST）
            # 明示的に +9h で変換
            from datetime import timezone, timedelta as td
            jst = timezone(td(hours=9))
            dt_jst = dt.astimezone(jst)
            dt_disp = dt_jst.strftime("%Y/%m/%d %H:%M:%S")
        except Exception:
            dt_disp = dt_str
    else:
        dt_disp = ""

    resource_type = ce.get("changeResourceType", "")
    operation     = ce.get("resourceChangeOperation", "")
    changed_fields = ce.get("changedFields", "")

    content = build_change_content(resource_type, operation, changed_fields)

    return {
        "日時":       dt_disp,
        "ユーザー":   ce.get("userEmail", ""),
        "キャンペーン": campaign.get("name", ""),
        "広告グループ": ad_group.get("name", ""),
        "変更内容":   content,
    }


# ============================================================
# CSV エクスポート
# ============================================================

def export_csv(rows: list, out_path: Path):
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write("変更履歴レポート\n")
        f.write(f"取得日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
        f.write("※ 変更内容の文言はAPIから生成したもので管理画面とは異なります\n")
        f.write("※ APIのデータ保持期間は直近30日間です\n")
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"✓ CSV保存: {out_path}  ({len(rows):,} 行)")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads 変更履歴取得")
    parser.add_argument("--site",     required=True, help="サイトID / アカウント名 / customer_id")
    parser.add_argument("--campaign", default=None,  help="キャンペーンIDまたは名前で絞り込み")
    parser.add_argument("--from",     dest="date_from", default=None,
                        help="開始日 YYYY-MM-DD（省略時: 30日前）")
    parser.add_argument("--to",       dest="date_to",   default=None,
                        help="終了日 YYYY-MM-DD（省略時: 今日）")
    args = parser.parse_args()

    # 日付デフォルト: 今日〜28日前（APIの保持上限は30日だが余裕を持って28日）
    today = datetime.today().strftime("%Y-%m-%d")
    default_from = (datetime.today() - timedelta(days=28)).strftime("%Y-%m-%d")
    date_from = args.date_from or default_from
    date_to   = args.date_to   or today

    creds   = load_credentials()
    account = load_account(args.site)
    cid     = account["customer_id"]

    campaign_id = None
    if args.campaign:
        campaign_id = resolve_campaign_id(account["site_id"], args.campaign)

    print(f"アカウント : {account['name']} ({cid})")
    print(f"期間       : {date_from} 〜 {date_to}")
    if campaign_id:
        print(f"キャンペーン: {campaign_id}")
    print()
    print("⚠️  注意: change_event APIのデータ保持期間は直近30日です")
    print("   30日以前のデータが必要な場合は管理画面からCSVをダウンロードしてください")
    print()

    token = get_access_token(creds)

    print("変更履歴取得中...")
    raw_rows = fetch_change_events(creds, token, cid, date_from, date_to, campaign_id)
    print(f"  取得件数: {len(raw_rows):,}")

    # 行変換
    csv_rows = [row_to_csv(r) for r in raw_rows]

    # 日時降順でソート（APIは順序保証なし）
    csv_rows.sort(key=lambda r: r["日時"], reverse=True)

    # ── 表示 ──────────────────────────────────────────────
    print(f"\n【変更履歴】{len(csv_rows)} 件")
    for row in csv_rows[:10]:
        content_first = row["変更内容"].split("\n")[0]
        print(f"  {row['日時']}  {row['キャンペーン'] or row['広告グループ'] or '-'}")
        print(f"    {content_first}")
    if len(csv_rows) > 10:
        print(f"  ... 他 {len(csv_rows) - 10} 件")

    # ── 保存 ──────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_out = OUTPUT_DIR / f"{account['site_id']}_change_history_{ts}.json"
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "account":    account,
            "period":     {"from": date_from, "to": date_to},
            "campaign_id": campaign_id,
            "fetched_at": datetime.now().isoformat(),
            "api_note":   "change_event は直近30日のデータのみ取得可能",
            "rows":       csv_rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON保存: {json_out}")

    csv_out = OUTPUT_DIR / f"{account['site_id']}_change_history_{ts}.csv"
    export_csv(csv_rows, csv_out)


if __name__ == "__main__":
    main()
