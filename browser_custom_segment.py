#!/usr/bin/env python3
"""
ブラウザ経由でカスタムセグメントの「除外キーワード」を取得するモジュール。

Google Ads API の custom_audience リソースには members（左側のキーワード）は含まれるが、
「セグメント分析の推定に含まれていないキーワード」（右側の黄色ボックス）は含まれない。
このモジュールはブラウザ経由でそれらを取得する。

使い方（メインエージェントから）:
    このモジュールは直接実行しない。
    メインエージェントが Chrome MCP ツール（navigate, read_page, find, computer 等）を使い、
    本モジュールの関数・定数を利用してブラウザ操作を行う。

フロー:
    1. prepare_custom_segment_tasks() でタスクリストを生成
    2. メインエージェントが各タスクの URL にブラウザでアクセス
    3. extract_excluded_keywords_js() の JS を実行して除外kwd を抽出
    4. merge_excluded_keywords() で API データと統合
    5. save_result() で JSON 保存
"""

import json
import glob
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from datetime import datetime


# ============================================================
# 定数
# ============================================================

# オーディエンスマネージャーのベース URL
AUDIENCE_MANAGER_BASE = "https://ads.google.com/aw/audiences/management/customaudience"


# ============================================================
# 設定
# ============================================================

@dataclass
class CustomSegmentConfig:
    """カスタムセグメント取得の設定"""
    site_id: str
    customer_id: str        # ハイフンなし: 1871050145
    ocid: str               # アカウントの ocid
    segment_id: str         # カスタムセグメントID
    segment_name: str       # セグメント名
    member_count: int = 0   # API取得済みのメンバー数

    @property
    def customer_id_clean(self) -> str:
        return self.customer_id.replace("-", "")


def _find_browser_config() -> Optional[dict]:
    """browser_config.json を自動検出して読み込む（.skills/キャッシュを除外）"""
    candidates = [
        p for p in (
            glob.glob("/sessions/*/mnt/*/GoogleAds_Fetcher/config/browser_config.json") +
            glob.glob("/sessions/*/mnt/GoogleAds_Fetcher/config/browser_config.json")
        ) if ".skills" not in p
    ]
    for path in candidates:
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            continue
    return None


def get_account_ocid(site_id: str) -> str:
    """browser_config.json からアカウントの ocid を取得"""
    config = _find_browser_config()
    if config and site_id in config.get("accounts", {}):
        return config["accounts"][site_id].get("ocid", "")
    return ""


# ============================================================
# タスク生成
# ============================================================

def prepare_custom_segment_tasks(
    site_id: str,
    audience_data: dict,
    ocid: str = "",
) -> list[dict]:
    """
    API取得済みのオーディエンスデータから、ブラウザ確認タスクリストを生成する。

    Args:
        site_id: サイトID (例: "072")
        audience_data: fetch_audience_settings で取得した JSON データ
        ocid: アカウントの ocid（省略時は browser_config.json から自動取得）

    Returns:
        [{"config": CustomSegmentConfig, "label": str}, ...]
        検索kwdタイプのセグメントのみ
    """
    if not ocid:
        ocid = get_account_ocid(site_id)
    if not ocid:
        raise ValueError(f"ocid が見つかりません（site_id={site_id}）。browser_config.json を確認してください。")

    customer_id = audience_data.get("account", {}).get("customer_id", "")
    custom_segments = audience_data.get("custom_segments", [])

    # 検索kwdタイプのみフィルタ
    search_kwd_segments = [
        s for s in custom_segments
        if s.get("member_type") == "検索キーワード"
    ]

    tasks = []
    for seg in search_kwd_segments:
        config = CustomSegmentConfig(
            site_id=site_id,
            customer_id=customer_id,
            ocid=ocid,
            segment_id=seg["セグメントID"],
            segment_name=seg["セグメント名"],
            member_count=len(seg.get("members", [])),
        )
        tasks.append({
            "config": config,
            "label": f"{seg['セグメント名']} (members={config.member_count})",
        })

    return tasks


def build_audience_manager_url(ocid: str) -> str:
    """オーディエンスマネージャーのカスタムセグメントタブの URL を生成"""
    return f"{AUDIENCE_MANAGER_BASE}?ocid={ocid}&ascid={ocid}"


# ============================================================
# ブラウザ操作用 JavaScript
# ============================================================

# 除外キーワードの黄色ボックスからテキストを抽出する JS
# 黄色ボックスは右上に表示され、「これらのキーワードはセグメント分析の推定に含まれていません。」
# というヘッダーの下にリスト形式で除外kwdが列挙される
#
# 改良版（2026-03-10）:
#   - リーフノードのみを検索して「推定に含まれていません」テキストを特定
#   - そこから親を辿り「すべて削除」を含むコンテナを見つけてから <li> を抽出
#   - 旧版は el.closest('[class]') で親を探していたが、DOM構造により失敗するケースがあった
JS_EXTRACT_EXCLUDED_KEYWORDS = """
(() => {
    const all = document.querySelectorAll('*');
    for (const el of all) {
        // リーフノード（子要素なし）のみチェック
        if (el.children.length > 0) continue;
        const t = (el.textContent || '').trim();
        if (t.includes('推定に含まれていません')) {
            // 親を辿って「すべて削除」を含むコンテナを探す
            let container = el.parentElement;
            for (let i = 0; i < 10 && container; i++) {
                const txt = container.innerText || '';
                if (txt.includes('すべて削除')) {
                    const keywords = [];
                    container.querySelectorAll('li').forEach(li => {
                        const kwd = li.textContent.trim();
                        if (kwd) keywords.push(kwd);
                    });
                    return JSON.stringify({
                        found: true,
                        keywords: keywords,
                        count: keywords.length
                    });
                }
                container = container.parentElement;
            }
            // ヘッダーは見つかったがコンテナが見つからない場合
            return JSON.stringify({found: true, keywords: [], count: 0});
        }
    }
    return JSON.stringify({found: false, keywords: [], count: 0});
})()
"""

# read_page のアクセシビリティツリーから除外キーワードを抽出するための
# 代替アプローチ（JS が動かない場合のフォールバック）
def parse_excluded_from_page_text(page_text: str) -> list[str]:
    """
    get_page_text() や read_page() のテキスト出力から除外キーワードを抽出する。

    除外キーワードは以下の形式で表示される:
        これらのキーワードはセグメント分析の推定に含まれていません。
        ・湘南 歯科 矯正
        ・ウィ スマイル 矯正
        ...
        すべて削除

    Args:
        page_text: ページのテキスト内容

    Returns:
        除外キーワードのリスト
    """
    lines = page_text.split("\n")
    in_section = False
    keywords = []

    for line in lines:
        line = line.strip()
        if "セグメント分析の推定に含まれていません" in line:
            in_section = True
            continue
        if in_section:
            if "すべて削除" in line:
                break
            # 先頭のbullet記号を除去
            cleaned = line.lstrip("・•- ").strip()
            if cleaned and len(cleaned) < 100:
                keywords.append(cleaned)

    return keywords


# ============================================================
# 結果の統合・保存
# ============================================================

def merge_excluded_keywords(
    audience_data: dict,
    excluded_map: dict[str, list[str]],
) -> dict:
    """
    API取得データにブラウザ取得の除外キーワードをマージする。

    Args:
        audience_data: fetch_audience_settings で取得した JSON データ
        excluded_map: {segment_id: [excluded_keyword, ...]} のマッピング

    Returns:
        マージ済みの結果 dict
    """
    result = {
        "account": audience_data.get("account", {}),
        "fetched_at": audience_data.get("fetched_at", ""),
        "browser_fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "segments": [],
        "summary": {
            "total_segments": 0,
            "segments_with_excluded": 0,
            "segments_without_excluded": 0,
            "total_excluded_keywords": 0,
        },
    }

    custom_segments = audience_data.get("custom_segments", [])
    search_kwd_segments = [
        s for s in custom_segments
        if s.get("member_type") == "検索キーワード"
    ]

    for seg in search_kwd_segments:
        seg_id = seg["セグメントID"]
        excluded = excluded_map.get(seg_id, [])

        entry = {
            "セグメントID": seg_id,
            "セグメント名": seg["セグメント名"],
            "member_type": "検索キーワード",
            "members": seg.get("members", []),
            "excluded_members": excluded,
            "member_count": len(seg.get("members", [])),
            "excluded_count": len(excluded),
        }
        result["segments"].append(entry)

    # サマリー計算
    result["summary"]["total_segments"] = len(result["segments"])
    result["summary"]["segments_with_excluded"] = sum(
        1 for s in result["segments"] if s["excluded_count"] > 0
    )
    result["summary"]["segments_without_excluded"] = sum(
        1 for s in result["segments"] if s["excluded_count"] == 0
    )
    result["summary"]["total_excluded_keywords"] = sum(
        s["excluded_count"] for s in result["segments"]
    )

    return result


def save_result(result: dict, data_dir: str | Path | None = None) -> Path:
    """
    結果を JSON ファイルに保存する。

    Args:
        result: merge_excluded_keywords() の結果
        data_dir: 保存先ディレクトリ（省略時は自動検出）

    Returns:
        保存されたファイルのパス
    """
    if data_dir is None:
        hits = [
            p for p in (
                glob.glob("/sessions/*/mnt/GoogleAds_Data") +
                glob.glob("/sessions/*/mnt/*/GoogleAds_Data")
            ) if ".skills" not in p
        ]
        if not hits:
            raise FileNotFoundError("GoogleAds_Data ディレクトリが見つかりません")
        data_dir = hits[0]

    data_dir = Path(data_dir)
    site_id = result["account"].get("site_id", "unknown")
    filename = f"{site_id}_custom_segment_excluded_keywords.json"
    filepath = data_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return filepath


# ============================================================
# メインエージェント用ヘルパー
# ============================================================

def load_latest_audience_data(site_id: str, data_dir: str | Path | None = None) -> dict:
    """
    最新の audience_settings JSON を読み込む。

    Args:
        site_id: サイトID
        data_dir: GoogleAds_Data ディレクトリ（省略時は自動検出）

    Returns:
        audience_settings の JSON データ
    """
    if data_dir is None:
        hits = [
            p for p in (
                glob.glob("/sessions/*/mnt/GoogleAds_Data") +
                glob.glob("/sessions/*/mnt/*/GoogleAds_Data")
            ) if ".skills" not in p
        ]
        if not hits:
            raise FileNotFoundError("GoogleAds_Data ディレクトリが見つかりません")
        data_dir = hits[0]

    data_dir = Path(data_dir)
    pattern = f"{site_id}_audience_settings_*.json"
    files = sorted(data_dir.glob(pattern), reverse=True)

    if not files:
        raise FileNotFoundError(
            f"{data_dir} に {pattern} が見つかりません。"
            f"先に fetch_audience_settings を実行してください。"
        )

    with open(files[0], encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# エージェント用プロンプトテンプレート
# ============================================================

AGENT_PROMPT_TEMPLATE = """
## カスタムセグメント除外キーワード取得フロー

### 概要
1. API で audience_settings を取得（検索kwdタイプのセグメント一覧 + members を取得）
2. ブラウザでオーディエンスマネージャーを開き、各セグメントの編集画面で
   右上の黄色ボックス「セグメント分析の推定に含まれていません」の有無を確認
3. 存在する場合は除外キーワードを読み取る
4. API データと統合して JSON 保存

### ステップ1: API データ取得
```python
import sys, glob
fetcher_dir = [p for p in glob.glob("/sessions/*/mnt/*/GoogleAds_Fetcher") if '.skills' not in p][0]
skill_dir = [p for p in glob.glob("/sessions/*/mnt/*/google-ads") if '.skills' not in p][0]
sys.path.insert(0, fetcher_dir)
sys.path.insert(0, skill_dir)

from trigger_fetch import fetch_and_read
data = fetch_and_read(site="{site_id}", action="fetch_audience_settings")
```

### ステップ2: タスクリスト生成
```python
from browser_custom_segment import (
    prepare_custom_segment_tasks,
    build_audience_manager_url,
    load_latest_audience_data,
    JS_EXTRACT_EXCLUDED_KEYWORDS,
    parse_excluded_from_page_text,
    merge_excluded_keywords,
    save_result,
)

audience_data = load_latest_audience_data("{site_id}")
tasks = prepare_custom_segment_tasks("{site_id}", audience_data)
manager_url = build_audience_manager_url(tasks[0]["config"].ocid)
```

### ステップ3: ブラウザ操作
1. manager_url にアクセス → カスタムセグメント一覧表示
2. 各セグメント名をクリック → 編集ダイアログ表示
3. read_page または find で黄色ボックスの有無を確認
4. 存在する場合: find("含まれていません") でテキスト取得、
   または javascript_tool で JS_EXTRACT_EXCLUDED_KEYWORDS を実行
5. キャンセルで閉じて次のセグメントへ

### ステップ4: 結果統合・保存
```python
excluded_map = {{}}  # {{segment_id: [keyword, ...]}}
# ブラウザ操作で取得した除外kwdを excluded_map に格納

result = merge_excluded_keywords(audience_data, excluded_map)
filepath = save_result(result)
print(f"保存先: {{filepath}}")
print(f"統計: {{result['summary']}}")
```
"""


def get_agent_prompt(site_id: str) -> str:
    """サブエージェント用のプロンプトを生成"""
    return AGENT_PROMPT_TEMPLATE.format(site_id=site_id)
