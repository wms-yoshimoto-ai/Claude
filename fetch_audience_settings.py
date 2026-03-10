#!/usr/bin/env python3
"""
Google Ads API v22 Audience Settings Fetcher
Retrieves audience settings (user_list, audience, custom_audience) from the management console.
Follows the settings-type fetcher pattern (no date range needed).
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import requests

from campaign_db import resolve_campaign_id


# ============================================================================
# Infrastructure Configuration
# ============================================================================

SCRIPT_DIR       = Path(__file__).parent
CREDENTIALS_FILE = SCRIPT_DIR / "config" / "credentials.json"
ACCOUNTS_FILE    = SCRIPT_DIR / "config" / "accounts.json"
OUTPUT_DIR       = Path.home() / "Documents" / "GoogleAds_Data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Enum Mappings
# ============================================================================

USER_LIST_TYPE_MAP = {
    "REMARKETING": "ウェブサイトを訪れたユーザー",
    "LOGICAL": "組み合わせリストのセグメント",
    "RULE_BASED": "ルール設定",
    "CRM_BASED": "購入ユーザー",
    "EXTERNAL_REMARKETING": "外部リマーケティング",
    "SIMILAR": "類似ユーザー",
}

MEMBERSHIP_STATUS_MAP = {
    "OPEN": "オープン",
    "CLOSED": "クローズ",
}

SIZE_RANGE_MAP = {
    "LESS_THAN_FIVE_HUNDRED": "500 未満",
    "LESS_THAN_ONE_THOUSAND": "1,000 未満",
    "ONE_THOUSAND_TO_TEN_THOUSAND": "1,000〜1万",
    "TEN_THOUSAND_TO_FIFTY_THOUSAND": "1万〜5万",
    "FIFTY_THOUSAND_TO_ONE_HUNDRED_THOUSAND": "5万〜10万",
    "ONE_HUNDRED_THOUSAND_TO_THREE_HUNDRED_THOUSAND": "10万〜30万",
    "THREE_HUNDRED_THOUSAND_TO_FIVE_HUNDRED_THOUSAND": "30万〜50万",
    "FIVE_HUNDRED_THOUSAND_TO_ONE_MILLION": "50万〜100万",
    "ONE_MILLION_TO_TWO_MILLION": "100万〜200万",
    "TWO_MILLION_TO_THREE_MILLION": "200万〜300万",
    "THREE_MILLION_TO_FIVE_MILLION": "300万〜500万",
    "FIVE_MILLION_TO_TEN_MILLION": "500万〜1,000万",
    "OVER_TEN_MILLION": "1,000万超",
    "UNSPECIFIED": " --",
    "UNKNOWN": " --",
}

CUSTOM_AUDIENCE_TYPE_MAP = {
    "AUTO": "自動",
    "INTEREST": "カスタム セグメント 興味 / 関心",
    "PURCHASE_INTENT": "カスタム セグメント 購買意向",
    "SEARCH": "カスタム セグメント 検索語句",
}

CUSTOM_AUDIENCE_MEMBER_TYPE_MAP = {
    "KEYWORD": "検索キーワード",
    "URL": "URL",
    "PLACE_CATEGORY": "場所のカテゴリ",
    "APP": "アプリ",
}

AUDIENCE_STATUS_MAP = {
    "ENABLED": "有効",
    "REMOVED": "削除済み",
}

GENDER_MAP = {
    "MALE": "男性",
    "FEMALE": "女性",
    "UNDETERMINED": "不明",
}

AGE_RANGE_MAP = {
    "AGE_RANGE_18_24": "18〜24",
    "AGE_RANGE_25_34": "25〜34",
    "AGE_RANGE_35_44": "35〜44",
    "AGE_RANGE_45_54": "45〜54",
    "AGE_RANGE_55_64": "55〜64",
    "AGE_RANGE_65_UP": "65才以上",
    "AGE_RANGE_UNDETERMINED": "不明",
}

PARENTAL_STATUS_MAP = {
    "PARENT": "子供あり",
    "NOT_A_PARENT": "子供なし",
    "UNDETERMINED": "不明",
}

INCOME_RANGE_MAP = {
    "INCOME_RANGE_0_50": "下位50%",
    "INCOME_RANGE_50_60": "60〜50%",
    "INCOME_RANGE_60_70": "70〜60%",
    "INCOME_RANGE_70_80": "80〜70%",
    "INCOME_RANGE_80_90": "90〜80%",
    "INCOME_RANGE_90_100": "上位10%",
    "INCOME_RANGE_UNDETERMINED": "不明",
}


# ============================================================================
# Authentication & Utilities
# ============================================================================

def load_credentials() -> dict:
    """Load credentials from config/credentials.json"""
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)["google_ads"]


def load_account(site_id: str) -> dict:
    """Load account info from config/accounts.json by site_id"""
    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        accounts = json.load(f)["accounts"]
    for acct in accounts:
        if acct.get("site_id") == site_id:
            return acct
    raise ValueError(f"サイトID '{site_id}' が accounts.json に見つかりません")


def get_access_token(creds: dict) -> str:
    """Obtain access token via OAuth2 refresh token"""
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     creds["oauth"]["client_id"],
            "client_secret": creds["oauth"]["client_secret"],
            "refresh_token": creds["oauth"]["refresh_token"],
            "grant_type":    "refresh_token",
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def gaql_request(customer_id: str, gaql: str, creds: dict, token: str) -> list:
    """Execute GAQL query and return results"""
    url = f"https://googleads.googleapis.com/v22/customers/{customer_id}/googleAds:searchStream"
    headers = {
        "Authorization":     f"Bearer {token}",
        "developer-token":   creds["developer_token"],
        "login-customer-id": creds["mcc_customer_id"],
        "Content-Type":      "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": gaql})
    if resp.status_code != 200:
        print(f"[ERROR] GAQL失敗: {resp.status_code}", file=sys.stderr)
        print(resp.text[:500], file=sys.stderr)
        return []
    results = []
    data = json.loads(resp.text)
    if isinstance(data, list):
        for batch in data:
            if isinstance(batch, dict):
                results.extend(batch.get("results", []))
    elif isinstance(data, dict):
        results.extend(data.get("results", []))
    return results


# ============================================================================
# GAQL Fetchers
# ============================================================================

def fetch_user_lists(customer_id: str, creds: dict, token: str) -> list:
    """Fetch user_list (データセグメント)"""
    gaql = """
SELECT
    user_list.id,
    user_list.name,
    user_list.type,
    user_list.description,
    user_list.membership_status,
    user_list.match_rate_percentage,
    user_list.size_for_display,
    user_list.size_for_search,
    user_list.size_range_for_display,
    user_list.size_range_for_search,
    user_list.eligible_for_display,
    user_list.eligible_for_search
FROM user_list
"""
    return gaql_request(customer_id, gaql, creds, token)


def fetch_audiences(customer_id: str, creds: dict, token: str) -> list:
    """Fetch audience (オーディエンス)"""
    gaql = """
SELECT
    audience.id,
    audience.name,
    audience.status,
    audience.description,
    audience.dimensions,
    audience.exclusion_dimension
FROM audience
"""
    return gaql_request(customer_id, gaql, creds, token)


def fetch_custom_audiences(customer_id: str, creds: dict, token: str) -> list:
    """Fetch custom_audience (カスタムセグメント)"""
    gaql = """
SELECT
    custom_audience.id,
    custom_audience.name,
    custom_audience.type,
    custom_audience.status,
    custom_audience.description,
    custom_audience.members
FROM custom_audience
"""
    return gaql_request(customer_id, gaql, creds, token)


def fetch_audience_signals(customer_id: str, campaign_filter: str, creds: dict, token: str) -> list:
    """Fetch asset_group_signal (使用先マッピング)"""
    gaql = f"""
SELECT
    asset_group_signal.audience.audience,
    asset_group.id,
    asset_group.name,
    campaign.id,
    campaign.name
FROM asset_group_signal
WHERE campaign.status != 'REMOVED'
  {campaign_filter}
"""
    return gaql_request(customer_id, gaql, creds, token)


# ============================================================================
# Data Transformation
# ============================================================================

def transform_data_segments(user_list_results: list) -> list:
    """Transform user_list results to output format"""
    data_segments = []
    for r in user_list_results:
        user_list = r.get("userList", {})

        # Determine size for search
        size_search = user_list.get("sizeForSearch")
        if not size_search or size_search == 0:
            size_range = user_list.get("sizeRangeForSearch", "UNSPECIFIED")
            size_search = SIZE_RANGE_MAP.get(size_range, " --")

        # Determine size for display
        size_display = user_list.get("sizeForDisplay")
        if not size_display or size_display == 0:
            size_range = user_list.get("sizeRangeForDisplay", "UNSPECIFIED")
            size_display = SIZE_RANGE_MAP.get(size_range, " --")

        if not size_search and not size_display:
            size_search = "配信するには小さすぎます"
            size_display = "配信するには小さすぎます"

        membership_status = user_list.get("membershipStatus", "UNSPECIFIED")
        row = {
            "セグメントID": user_list.get("id", ""),
            "セグメント名": user_list.get("name", ""),
            "セグメントタイプ": USER_LIST_TYPE_MAP.get(user_list.get("type", ""), ""),
            "ステータス": MEMBERSHIP_STATUS_MAP.get(membership_status, membership_status),
            "サイズ_検索": size_search,
            "サイズ_ディスプレイ": size_display,
            "説明": user_list.get("description", ""),
            "_type_raw": user_list.get("type", ""),
        }
        data_segments.append(row)

    return data_segments


def transform_custom_segments(custom_audience_results: list) -> list:
    """Transform custom_audience results to output format"""
    custom_segments = []
    for r in custom_audience_results:
        custom_audience = r.get("customAudience", {})

        members = custom_audience.get("members", [])
        member_values = []
        member_type_raw = ""

        if members:
            member_type_raw = members[0].get("memberType", "")
            for member in members:
                if member_type_raw == "KEYWORD" and "keyword" in member:
                    member_values.append(member["keyword"])
                elif member_type_raw == "URL" and "url" in member:
                    member_values.append(member["url"])
                elif member_type_raw == "PLACE_CATEGORY" and "placeCategory" in member:
                    member_values.append(member["placeCategory"])
                elif member_type_raw == "APP" and "app" in member:
                    member_values.append(member["app"])

        status = custom_audience.get("status", "ENABLED")
        row = {
            "セグメントID": custom_audience.get("id", ""),
            "セグメント名": custom_audience.get("name", ""),
            "タイプ": CUSTOM_AUDIENCE_TYPE_MAP.get(custom_audience.get("type", ""), ""),
            "ステータス": "有効" if status == "ENABLED" else status,
            "members": member_values,
            "member_type": CUSTOM_AUDIENCE_MEMBER_TYPE_MAP.get(member_type_raw, ""),
            "_type_raw": custom_audience.get("type", ""),
        }
        custom_segments.append(row)

    return custom_segments


def extract_resource_id(resource_name: str) -> str:
    """Extract ID from resource name like 'customers/123/userLists/456'"""
    parts = resource_name.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return resource_name


def build_lookup_maps(user_list_results: list, custom_audience_results: list) -> tuple:
    """Build lookup maps for resolving resource names to names"""
    user_list_map = {}
    custom_audience_map = {}

    for r in user_list_results:
        user_list = r.get("userList", {})
        list_id = str(user_list.get("id", ""))
        if list_id:
            user_list_map[list_id] = {
                "name": user_list.get("name", ""),
                "type": user_list.get("type", ""),
            }

    for r in custom_audience_results:
        custom_audience = r.get("customAudience", {})
        aud_id = str(custom_audience.get("id", ""))
        if aud_id:
            custom_audience_map[aud_id] = {
                "name": custom_audience.get("name", ""),
                "type": custom_audience.get("type", ""),
            }

    return user_list_map, custom_audience_map


def parse_audience_dimensions(audience: dict, user_list_map: dict, custom_audience_map: dict) -> dict:
    """Parse complex audience.dimensions and exclusion_dimension fields"""
    dimensions = audience.get("dimensions", [])
    exclusion_dimension = audience.get("exclusionDimension", [])

    custom_segments = []
    user_data = []
    interests = []
    exclusions = []
    user_attributes = {
        "性別": [],
        "年齢": [],
        "子供の有無": [],
        "世帯収入": [],
    }

    def _process_segment(seg: dict):
        """audienceSegments.segments 内の1セグメントを分類"""
        if not isinstance(seg, dict):
            return
        if "customAudience" in seg:
            resource_name = seg["customAudience"].get("customAudience", "")
            aud_id = extract_resource_id(resource_name)
            if aud_id in custom_audience_map:
                custom_segments.append(custom_audience_map[aud_id]["name"])
        elif "userList" in seg:
            resource_name = seg["userList"].get("userList", "")
            list_id = extract_resource_id(resource_name)
            if list_id in user_list_map:
                info = user_list_map[list_id]
                user_data.append(f"{info['name']} ({USER_LIST_TYPE_MAP.get(info['type'], info['type'])})")
            else:
                user_data.append(f"[未解決] userList/{list_id}")
        elif "userInterest" in seg:
            resource_name = seg["userInterest"].get("userInterestCategory", "")
            if resource_name:
                interest_id = extract_resource_id(resource_name)
                interests.append({"type": "userInterest", "id": interest_id, "resource": resource_name})
        elif "detailedDemographic" in seg:
            resource_name = seg["detailedDemographic"].get("detailedDemographic", "")
            if resource_name:
                demo_id = extract_resource_id(resource_name)
                interests.append({"type": "detailedDemographic", "id": demo_id, "resource": resource_name})
        elif "lifeEvent" in seg:
            resource_name = seg["lifeEvent"].get("lifeEvent", "")
            if resource_name:
                event_id = extract_resource_id(resource_name)
                interests.append({"type": "lifeEvent", "id": event_id, "resource": resource_name})

    def _process_dimension(dim: dict):
        """1つの dimension オブジェクトを処理"""
        if not isinstance(dim, dict):
            return

        # audienceSegments: segments 配列の中に各セグメントタイプがネスト
        if "audienceSegments" in dim:
            segments = dim["audienceSegments"].get("segments", [])
            for seg in segments:
                _process_segment(seg)

        # age: ageRanges 配列（minAge/maxAge 形式）
        elif "age" in dim:
            age_info = dim["age"]
            age_ranges = age_info.get("ageRanges", [])
            for ar in age_ranges:
                min_age = ar.get("minAge", "?")
                max_age = ar.get("maxAge", "?")
                age_str = f"{min_age}〜{max_age}歳"
                if age_str not in user_attributes["年齢"]:
                    user_attributes["年齢"].append(age_str)
            if age_info.get("includeUndetermined"):
                undetermined = "不明を含む"
                if undetermined not in user_attributes["年齢"]:
                    user_attributes["年齢"].append(undetermined)

        # gender
        elif "gender" in dim:
            gender_info = dim["gender"]
            genders = gender_info.get("genders", [])
            for g in genders:
                gender_type = g.get("type", "UNDETERMINED")
                gender_jp = GENDER_MAP.get(gender_type, gender_type)
                if gender_jp not in user_attributes["性別"]:
                    user_attributes["性別"].append(gender_jp)
            if gender_info.get("includeUndetermined"):
                if "不明を含む" not in user_attributes["性別"]:
                    user_attributes["性別"].append("不明を含む")

        # parentalStatus
        elif "parentalStatus" in dim:
            ps_info = dim["parentalStatus"]
            statuses = ps_info.get("parentalStatuses", [])
            for ps in statuses:
                ps_type = ps.get("type", "UNDETERMINED")
                ps_jp = PARENTAL_STATUS_MAP.get(ps_type, ps_type)
                if ps_jp not in user_attributes["子供の有無"]:
                    user_attributes["子供の有無"].append(ps_jp)

        # incomeRange
        elif "incomeRange" in dim:
            ir_info = dim["incomeRange"]
            ranges = ir_info.get("incomeRanges", [])
            for ir in ranges:
                ir_type = ir.get("type", "INCOME_RANGE_UNDETERMINED")
                ir_jp = INCOME_RANGE_MAP.get(ir_type, ir_type)
                if ir_jp not in user_attributes["世帯収入"]:
                    user_attributes["世帯収入"].append(ir_jp)

    # Process dimensions
    if isinstance(dimensions, list):
        for dim in dimensions:
            _process_dimension(dim)

    # Process exclusion_dimension
    def _process_exclusion_segment(seg: dict):
        if not isinstance(seg, dict):
            return
        if "customAudience" in seg:
            resource_name = seg["customAudience"].get("customAudience", "")
            aud_id = extract_resource_id(resource_name)
            if aud_id in custom_audience_map:
                exclusions.append(custom_audience_map[aud_id]["name"])
        elif "userList" in seg:
            resource_name = seg["userList"].get("userList", "")
            list_id = extract_resource_id(resource_name)
            if list_id in user_list_map:
                exclusions.append(user_list_map[list_id]["name"])

    if isinstance(exclusion_dimension, list):
        for dim in exclusion_dimension:
            if not isinstance(dim, dict):
                continue
            if "audienceSegments" in dim:
                segments = dim["audienceSegments"].get("segments", [])
                for seg in segments:
                    _process_exclusion_segment(seg)
            else:
                _process_exclusion_segment(dim)

    return {
        "カスタムセグメント": custom_segments,
        "広告主様のデータ": user_data,
        "興味_関心": interests,
        "除外": exclusions,
        "ユーザー属性": user_attributes,
    }


def transform_audiences(audience_results: list, signal_results: list, user_list_map: dict, custom_audience_map: dict) -> list:
    """Transform audience results to output format"""
    # Build audience ID to usage mapping
    audience_usage = {}
    for signal in signal_results:
        aud_signal = signal.get("assetGroupSignal", {}).get("audience", {})
        aud_resource = aud_signal.get("audience", "")
        aud_id = extract_resource_id(aud_resource)

        if aud_id not in audience_usage:
            audience_usage[aud_id] = []

        agg = signal.get("assetGroup", {})
        campaign = signal.get("campaign", {})
        usage_info = {
            "asset_group_id": agg.get("id", ""),
            "asset_group_name": agg.get("name", ""),
            "campaign_id": campaign.get("id", ""),
            "campaign_name": campaign.get("name", ""),
        }
        audience_usage[aud_id].append(usage_info)

    audiences = []
    for r in audience_results:
        audience = r.get("audience", {})
        aud_id = str(audience.get("id", ""))

        # Parse dimensions
        parsed = parse_audience_dimensions(audience, user_list_map, custom_audience_map)

        # Get usage info
        usage_list = audience_usage.get(aud_id, [])
        usage_str = []
        for usage in usage_list:
            usage_str.append(f"{usage['campaign_name']} > {usage['asset_group_name']}")

        status = audience.get("status", "ENABLED")
        row = {
            "オーディエンスID": aud_id,
            "オーディエンス名": audience.get("name", ""),
            "ステータス": AUDIENCE_STATUS_MAP.get(status, status),
            "追加先": usage_str,
            "dimensions_raw": audience.get("dimensions", []),
            "カスタムセグメント": parsed["カスタムセグメント"],
            "広告主様のデータ": parsed["広告主様のデータ"],
            "興味_関心": parsed["興味_関心"],
            "除外": parsed["除外"],
            "ユーザー属性": parsed["ユーザー属性"],
        }
        audiences.append(row)

    return audiences


# ============================================================================
# Save Functions
# ============================================================================

def save_json(data_segments: list, audiences: list, custom_segments: list, account: dict, path: Path):
    """Save data to JSON file"""
    output = {
        "account": {
            "site_id": account.get("site_id", ""),
            "name": account.get("name", ""),
        },
        "fetched_at": datetime.now().isoformat(),
        "data_segments": data_segments,
        "audiences": audiences,
        "custom_segments": custom_segments,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def save_csv(data_segments: list, audiences: list, custom_segments: list, path: Path):
    """Save data to CSV file with 3 sections"""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)

        # Section 1: データセグメント
        writer.writerow(["[データセグメント]"])
        ds_cols = ["セグメントID", "セグメント名", "セグメントタイプ", "ステータス", "サイズ_検索", "サイズ_ディスプレイ", "説明"]
        writer.writerow(ds_cols)
        for r in data_segments:
            writer.writerow([r.get(c, "") for c in ds_cols])

        writer.writerow([])  # blank line

        # Section 2: オーディエンス
        writer.writerow(["[オーディエンス]"])
        aud_cols = ["オーディエンスID", "オーディエンス名", "ステータス", "追加先", "カスタムセグメント", "広告主様のデータ", "興味_関心", "除外", "ユーザー属性"]
        writer.writerow(aud_cols)
        for r in audiences:
            row = []
            for c in aud_cols:
                v = r.get(c, "")
                if isinstance(v, (list, dict)):
                    v = json.dumps(v, ensure_ascii=False)
                row.append(v)
            writer.writerow(row)

        writer.writerow([])

        # Section 3: カスタムセグメント
        writer.writerow(["[カスタムセグメント]"])
        cs_cols = ["セグメントID", "セグメント名", "タイプ", "ステータス", "member_type", "members"]
        writer.writerow(cs_cols)
        for r in custom_segments:
            row = [r.get(c, "") for c in cs_cols[:-1]]
            members = r.get("members", [])
            row.append("; ".join(members) if isinstance(members, list) else str(members))
            writer.writerow(row)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Google Ads オーディエンス設定取得")
    parser.add_argument("--site", required=True, help="サイトID")
    parser.add_argument("--campaign", default="", help="キャンペーンフィルタ")
    parser.add_argument("--from", dest="date_from", default="", help="unused, for compatibility")
    parser.add_argument("--to", dest="date_to", default="", help="unused, for compatibility")
    args = parser.parse_args()

    creds = load_credentials()
    acct = load_account(args.site)
    cid = acct["customer_id"].replace("-", "")
    token = get_access_token(creds)

    campaign_id = ""
    campaign_filter = ""
    if args.campaign:
        campaign_id = resolve_campaign_id(args.site, args.campaign)
        if campaign_id:
            campaign_filter = f"AND campaign.id = {campaign_id}"

    print(f"[INFO] サイト: {args.site} / 顧客ID: {cid}")

    # Fetch all 4 queries
    print("[INFO] データセグメント（user_list）を取得中...")
    user_list_results = fetch_user_lists(cid, creds, token)
    print(f"  → {len(user_list_results)} 件")

    print("[INFO] オーディエンス（audience）を取得中...")
    audience_results = fetch_audiences(cid, creds, token)
    print(f"  → {len(audience_results)} 件")

    print("[INFO] カスタムセグメント（custom_audience）を取得中...")
    custom_audience_results = fetch_custom_audiences(cid, creds, token)
    print(f"  → {len(custom_audience_results)} 件")

    print("[INFO] 使用先マッピング（asset_group_signal）を取得中...")
    signal_results = fetch_audience_signals(cid, campaign_filter, creds, token)
    print(f"  → {len(signal_results)} 件")

    # Build lookup maps
    user_list_map, custom_audience_map = build_lookup_maps(user_list_results, custom_audience_results)

    # Transform data
    data_segments = transform_data_segments(user_list_results)
    custom_segments = transform_custom_segments(custom_audience_results)
    audiences = transform_audiences(audience_results, signal_results, user_list_map, custom_audience_map)

    # Save
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUTPUT_DIR / f"{args.site}_audience_settings_{ts}.json"
    csv_path = OUTPUT_DIR / f"{args.site}_audience_settings_{ts}.csv"

    save_json(data_segments, audiences, custom_segments, acct, json_path)
    save_csv(data_segments, audiences, custom_segments, csv_path)

    print(f"[JSON] {json_path}")
    print(f"[CSV] {csv_path}")
    print("[INFO] 完了")


if __name__ == "__main__":
    main()
