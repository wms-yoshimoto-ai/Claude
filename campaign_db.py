#!/usr/bin/env python3
"""
campaign_db.py - キャンペーンマスターDB操作モジュール

config/campaigns.json を読み書きする共通モジュール。
fetch_*.py スクリプトやCoworkセッションから import して使う。

【基本的な使い方】
  from campaign_db import resolve_campaign_id, list_campaigns
  from campaign_db import add_campaign, rename_campaign

【キャンペーンIDの解決】
  # キャンペーンID (数字) → そのまま返す
  cid = resolve_campaign_id("065", "23335569301")   # → "23335569301"

  # キャンペーン名 → IDに解決
  cid = resolve_campaign_id("065", "065 矯正 all（札幌市）")  # → "23335569301"

  # 部分一致（1件のみの場合）
  cid = resolve_campaign_id("065", "矯正 all")  # → "23335569301"

【一覧取得】
  # サイトの全キャンペーン
  camps = list_campaigns("065")
  # 全サイト
  camps = list_campaigns()

【追加・変更（別AIセッションからも実行可）】
  add_campaign("065", "23999999999", "065 新キャンペーン", "検索")
  rename_campaign("23335569301", "065 矯正 all（札幌市）新版")
  # 変更後は git_push でGitHubに反映する
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# campaigns.json のパス（このスクリプトと同じ config/ ディレクトリ）
_SCRIPT_DIR = Path(__file__).parent
CAMPAIGNS_FILE = _SCRIPT_DIR / "config" / "campaigns.json"


# ============================================================
# 内部ヘルパー
# ============================================================

def _load() -> list:
    """campaigns.json を読み込んでリストを返す"""
    if not CAMPAIGNS_FILE.exists():
        return []
    with open(CAMPAIGNS_FILE, encoding="utf-8") as f:
        return json.load(f).get("campaigns", [])


def _save(campaigns: list):
    """campaigns.json に書き込む"""
    CAMPAIGNS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CAMPAIGNS_FILE, "w", encoding="utf-8") as f:
        json.dump({"campaigns": campaigns}, f, ensure_ascii=False, indent=2)


# ============================================================
# 参照系
# ============================================================

def list_campaigns(site_id: str = None, campaign_type: str = None) -> list:
    """
    キャンペーン一覧を返す。

    Args:
        site_id:       "065" など。None なら全サイト
        campaign_type: "検索" / "P-MAX" / "デマンド ジェネレーション" など。None なら全タイプ

    Returns:
        [{"site_id", "campaign_name", "campaign_id", "campaign_type"}, ...]
    """
    camps = _load()
    if site_id:
        camps = [c for c in camps if c["site_id"] == site_id]
    if campaign_type:
        camps = [c for c in camps if c["campaign_type"] == campaign_type]
    return camps


def find_by_id(campaign_id: str) -> dict | None:
    """
    キャンペーンIDでキャンペーン情報を返す。

    Args:
        campaign_id: 数字のみの文字列 (例: "23335569301")

    Returns:
        {"site_id", "campaign_name", "campaign_id", "campaign_type"} または None
    """
    cid = str(campaign_id).replace("-", "").replace(" ", "")
    for c in _load():
        if c["campaign_id"] == cid:
            return c
    return None


def find_by_name(site_id: str, name_query: str) -> list:
    """
    キャンペーン名（完全一致または部分一致）でキャンペーンを検索する。

    Args:
        site_id:    サイトID
        name_query: キャンペーン名（完全一致 → 部分一致の順で検索）

    Returns:
        マッチしたキャンペーンのリスト
    """
    camps = [c for c in _load() if c["site_id"] == site_id]
    # 完全一致
    exact = [c for c in camps if c["campaign_name"] == name_query]
    if exact:
        return exact
    # 部分一致
    return [c for c in camps if name_query in c["campaign_name"]]


def resolve_campaign_id(site_id: str, query: str) -> str:
    """
    キャンペーンID（数字）またはキャンペーン名を受け取り、キャンペーンIDを返す。
    fetch_*.py の --campaign 引数の解決に使う。

    Args:
        site_id: "065" など
        query:   数字ID ("23335569301") またはキャンペーン名 ("065 矯正 all（札幌市）")

    Returns:
        キャンペーンID文字列

    Raises:
        ValueError: キャンペーンが見つからない、または複数候補がある場合
    """
    q = str(query).strip()

    # 数字のみ → IDとみなしそのまま返す（DB登録不要）
    if q.isdigit():
        return q

    # 名前で検索
    matches = find_by_name(site_id, q)
    if not matches:
        raise ValueError(
            f"キャンペーンが見つかりません: site_id={site_id} query='{q}'\n"
            f"登録済みキャンペーン: {[c['campaign_name'] for c in list_campaigns(site_id)]}"
        )
    if len(matches) > 1:
        names = [c["campaign_name"] for c in matches]
        raise ValueError(
            f"複数のキャンペーンが候補: {names}\n"
            f"より具体的な名前またはキャンペーンIDを指定してください"
        )
    return matches[0]["campaign_id"]


# ============================================================
# 更新系（別AIセッションからも呼び出し可）
# ============================================================

def add_campaign(site_id: str, campaign_id: str, campaign_name: str,
                 campaign_type: str = "検索") -> dict:
    """
    キャンペーンを追加する。

    Args:
        site_id:       "065"
        campaign_id:   "23999999999"
        campaign_name: "065 新キャンペーン（札幌市）"
        campaign_type: "検索" / "P-MAX" / "デマンド ジェネレーション" (default: "検索")

    Returns:
        追加したキャンペーンのdict

    Example (別AIセッションから):
        from campaign_db import add_campaign
        add_campaign("065", "23999999999", "065 新キャンペーン", "検索")
    """
    camps = _load()
    cid = str(campaign_id).strip()

    # 重複チェック
    existing = [c for c in camps if c["campaign_id"] == cid]
    if existing:
        raise ValueError(f"campaign_id={cid} はすでに登録されています: {existing[0]}")

    new_entry = {
        "site_id": str(site_id).strip(),
        "campaign_name": str(campaign_name).strip(),
        "campaign_id": cid,
        "campaign_type": str(campaign_type).strip(),
    }
    camps.append(new_entry)
    _save(camps)
    print(f"✓ キャンペーン追加: {new_entry}")
    return new_entry


def rename_campaign(campaign_id: str, new_name: str) -> dict:
    """
    キャンペーン名を変更する。

    Args:
        campaign_id: "23335569301"
        new_name:    "065 矯正 all（札幌市）新版"

    Returns:
        変更後のキャンペーンdict

    Example (別AIセッションから):
        from campaign_db import rename_campaign
        rename_campaign("23335569301", "065 矯正 all（札幌市）新版")
    """
    camps = _load()
    cid = str(campaign_id).strip()
    target = None
    for c in camps:
        if c["campaign_id"] == cid:
            target = c
            break
    if target is None:
        raise ValueError(f"campaign_id={cid} が見つかりません")

    old_name = target["campaign_name"]
    target["campaign_name"] = str(new_name).strip()
    _save(camps)
    print(f"✓ キャンペーン名変更: '{old_name}' → '{new_name}'")
    return target


def remove_campaign(campaign_id: str) -> dict:
    """
    キャンペーンをDBから削除する（実際のGoogle Ads管理画面には影響しない）。

    Args:
        campaign_id: "23335569301"

    Returns:
        削除したキャンペーンのdict
    """
    camps = _load()
    cid = str(campaign_id).strip()
    target = None
    remaining = []
    for c in camps:
        if c["campaign_id"] == cid:
            target = c
        else:
            remaining.append(c)
    if target is None:
        raise ValueError(f"campaign_id={cid} が見つかりません")
    _save(remaining)
    print(f"✓ キャンペーン削除: {target}")
    return target


# ============================================================
# CLI（直接実行 or run_fetcher.sh 経由）
# ============================================================

def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="キャンペーンマスターDB操作")
    sub = parser.add_subparsers(dest="cmd")

    # list
    p_list = sub.add_parser("list", help="キャンペーン一覧表示")
    p_list.add_argument("--site", default=None, help="サイトIDで絞り込み")
    p_list.add_argument("--type", default=None, dest="ctype", help="タイプで絞り込み")

    # add
    p_add = sub.add_parser("add", help="キャンペーン追加")
    p_add.add_argument("--site",   required=True, help="サイトID")
    p_add.add_argument("--id",     required=True, dest="cid", help="キャンペーンID")
    p_add.add_argument("--name",   required=True, help="キャンペーン名")
    p_add.add_argument("--type",   default="検索", dest="ctype", help="キャンペーンタイプ")

    # rename
    p_rename = sub.add_parser("rename", help="キャンペーン名変更")
    p_rename.add_argument("--id",      required=True, dest="cid", help="キャンペーンID")
    p_rename.add_argument("--new-name", required=True, dest="new_name", help="新しいキャンペーン名")

    # remove
    p_remove = sub.add_parser("remove", help="キャンペーン削除")
    p_remove.add_argument("--id", required=True, dest="cid", help="キャンペーンID")

    # find
    p_find = sub.add_parser("find", help="キャンペーン検索")
    p_find.add_argument("--site",  required=True, help="サイトID")
    p_find.add_argument("--query", required=True, help="IDまたは名前")

    args = parser.parse_args()

    if args.cmd == "list":
        camps = list_campaigns(args.site, args.ctype)
        print(f"{'サイト':<6} {'キャンペーンID':<16} {'タイプ':<22} キャンペーン名")
        print("-" * 80)
        for c in camps:
            print(f"{c['site_id']:<6} {c['campaign_id']:<16} {c['campaign_type']:<22} {c['campaign_name']}")
        print(f"\n合計: {len(camps)} 件")

    elif args.cmd == "add":
        add_campaign(args.site, args.cid, args.name, args.ctype)

    elif args.cmd == "rename":
        rename_campaign(args.cid, args.new_name)

    elif args.cmd == "remove":
        confirm = input(f"campaign_id={args.cid} を削除しますか？ (yes/no): ")
        if confirm.lower() == "yes":
            remove_campaign(args.cid)
        else:
            print("キャンセルしました")

    elif args.cmd == "find":
        cid = resolve_campaign_id(args.site, args.query)
        info = find_by_id(cid)
        if info:
            print(f"✓ {info}")
        else:
            print(f"✓ campaign_id={cid} (DBに詳細情報なし)")

    else:
        parser.print_help()


if __name__ == "__main__":
    _cli()
