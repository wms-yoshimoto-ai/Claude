# 指示書: ブラウザ経由の除外KWリスト↔キャンペーン マッピング取得

## 背景

除外キーワードリストに紐付いたキャンペーンの情報を、API（`campaign_shared_set`）で取得する仕組みを構築した。

**APIの取得能力（072アカウント 2026-03-11 確認済み）:**
- `campaign_shared_set` は **検索キャンペーンとPmaxキャンペーンの両方** を返す
- 072では P-MAX 3件、検索 17件、合計 20件のマッピングが取得できた
- `campaign_criterion` (KEYWORD) は検索キャンペーンのみ（Pmaxでは個別除外KWは取得不可）

**注意**: 前セッションでは `fetch_negative_keyword.py` に3つのバグ（credential key、JSON parse、引数順序）が
あり、その修正前にPmax取得テストを行ったため「Pmax除外KWリストはAPIで取得不可」と誤判定していた。
バグ修正後はPmaxも正常に取得できている。

**ブラウザ経由の検証・補完が有効な場面:**

1. 全アカウントで同様にPmaxが返る保証がない（072以外で0件の可能性を排除できない）
2. 管理画面では「キャンペーンに適用」タブで確実に確認できる
3. API結果とブラウザ結果のクロスチェックで信頼性向上

---

## 目的

管理画面の除外キーワードリスト詳細画面から「キャンペーンに適用」タブの情報をブラウザ経由で取得し、API結果と統合する。

---

## 取得フロー

### Step 1: 除外KWリスト一覧の取得

管理画面パス: `Google Ads > ツールと設定 > 共有ライブラリ > 除外キーワード リスト`

URL例: `https://ads.google.com/aw/negkeywordlists?ocid=xxxxxxxxxx`

取得情報:
- リスト名
- キーワード数
- 利用しているキャンペーン数

### Step 2: 各リスト詳細画面から「キャンペーンに適用」タブ

各リストをクリック → 「キャンペーンに適用」タブを選択

取得情報:
- 適用されているキャンペーン名
- キャンペーンID（URLまたはDOM属性から取得可能な場合）

### Step 3: キャンペーンタイプの判定

campaign_db（`GoogleAds_Fetcher/config/campaigns.json`）を参照してキャンペーンタイプを判定する。

```python
from campaign_db import list_campaigns
camps = list_campaigns(site_id)
# campaign_name → campaign_type のマップを構築
```

campaign_db にない場合は、キャンペーン名に "Pmax" / "P-MAX" が含まれるかで推定する。

---

## 出力JSON仕様

**ファイル名**: `{site_id}_negative_keyword_list_campaigns_browser.json`

**保存先**: `~/Documents/GoogleAds_Data/`

**構造** (API版と同一スキーマ):

```json
{
  "site_id": "072",
  "fetched_at": "2026-03-11T10:00:00.000000",
  "source": "browser",
  "total_mappings": 23,
  "campaign_type_distribution": {
    "検索": 17,
    "P-MAX": 6
  },
  "mappings": [
    {
      "list_name": "072 地域",
      "campaign_id": "23354271498",
      "campaign_name": "072 矯正 all（沖縄）",
      "campaign_type": "検索",
      "source": "browser"
    },
    {
      "list_name": "072 地域",
      "campaign_id": "23367869010",
      "campaign_name": "072 矯正 Pmax（沖縄）",
      "campaign_type": "P-MAX",
      "source": "browser"
    }
  ]
}
```

**重要なフィールド**:
- `source`: 必ず `"browser"` とする
- `campaign_type`: `"検索"` または `"P-MAX"` のいずれか
- `campaign_id`: 取得できない場合は空文字 `""` でも可（campaign_name で紐付け可能）

---

## マージ手順

ブラウザ版JSONを取得した後、API版とマージする。

```python
import sys
sys.path.insert(0, "path/to/GoogleAds_Fetcher")
from fetch_negative_keyword import merge_browser_data

# API版（既に存在するはず）
api_path = "/path/to/GoogleAds_Data/072_negative_keyword_list_campaigns.json"
# ブラウザ版（この指示書に基づいて作成したもの）
browser_path = "/path/to/GoogleAds_Data/072_negative_keyword_list_campaigns_browser.json"

merged_path = merge_browser_data(api_path, browser_path)
# → api_path が上書きされ、source="merged" となる
```

マージロジック:
- キー = `(list_name, campaign_id)` で一意化
- 同一キーが存在する場合はブラウザ版が後勝ち
- マージ後のJSONには `api_fetched_at` と `browser_fetched_at` の両方が記録される

---

## DB投入

マージ後、gads_db.py でDBに投入する。

```python
import json
sys.path.insert(0, "path/to/google-ads")
from gads_db import GadsDB, set_data_dir

set_data_dir("/path/to/GoogleAds_Data")
db = GadsDB("072")
db.init()

with open(merged_path) as f:
    data = json.load(f)

count = db.import_negative_keyword_list_campaigns(data)
print(f"Imported: {count} rows")

# 検証クエリ
print(db.get_campaigns_by_list("072 地域"))
print(db.get_lists_by_campaign("Pmax"))
```

---

## テスト基準（072アカウント）

### API取得結果（現状確認済み）

| キャンペーン | タイプ | リスト数 |
|---|---|---|
| 072 矯正 all（沖縄） | 検索 | 8 |
| 072 矯正 症状（沖縄） | 検索 | 7 |
| 072 矯正 Pmax（沖縄） | P-MAX | 3 |
| 合計 | | 20（うちPmax 3件、検索 17件） |

### ブラウザ取得で期待される結果

管理画面で確認した内容と一致すること。特に:

1. **「072 地域」リスト**: 検索2件（all, 症状）が表示されるはず。Pmaxがあればボーナス
2. **Pmaxキャンペーン**: API版と同数以上のリスト紐付けが確認できること
3. **検索キャンペーンはAPI版と完全一致すること**（ブラウザで追加発見されるケースは稀）

### クロスチェック

マージ後に以下を確認:
```python
# マージ後のtotal_mappings >= API版のtotal_mappings（ブラウザで追加発見分）
# campaign_type_distribution のP-MAX件数 >= API版のP-MAX件数
```

---

## 注意事項

1. 除外KWリスト画面のURLは管理画面のOCIDに依存するため、アカウント切り替え時に注意
2. 「キャンペーンに適用」タブが空の場合（どのキャンペーンにも適用されていないリスト）はスキップ可
3. campaign_id が取得困難な場合は campaign_name のみで記録し、DB投入時に campaign_db で解決する

---

## 関連ファイルパス

```
# 実装済み（API側）
GoogleAds_Fetcher/fetch_negative_keyword.py   ← extract_list_campaign_mappings(), save_list_campaign_mapping_json(), merge_browser_data()
google-ads/gads_db.py                          ← negative_keyword_list_campaigns テーブル, import/query メソッド

# 参照
GoogleAds_Fetcher/config/accounts.json         ← アカウント情報
GoogleAds_Fetcher/config/campaigns.json        ← キャンペーンマスター（campaign_db）

# 出力データ
~/Documents/GoogleAds_Data/{site_id}_negative_keyword_list_campaigns.json          ← API版（マージ先）
~/Documents/GoogleAds_Data/{site_id}_negative_keyword_list_campaigns_browser.json  ← ブラウザ版（マージ元）

# 既存のブラウザスクリプト参考
google-ads/browser_custom_segment.py           ← ブラウザ取得スクリプトのパターン参照
```
