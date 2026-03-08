# launchd セットアップ手順
## 「Cowork から Mac のデータ取得を起動する」仕組みの設定

---

## 概要

```
[Cowork] 「065のデータを取得して」と指示
    ↓ fetch_trigger.json を書き込む
[Mac launchd] ファイル変更を検知 → run_fetcher.sh を自動実行
    ↓ Google Ads API 呼び出し
[Mac] JSON を ~/Documents/GoogleAds_Data/ に保存
    ↓
[Cowork] 完了を確認 → JSON を読んで分析・レポート生成
```

---

## セットアップ手順（1回だけ実行）

### Step 1: run_fetcher.sh に実行権限を付与

Macのターミナルで実行：

```bash
chmod +x ~/Desktop/Claude/GoogleAds_Fetcher/run_fetcher.sh
```

### Step 2: launchd に plist を登録

```bash
cp ~/Desktop/Claude/GoogleAds_Fetcher/com.googleads.fetcher.plist \
   ~/Library/LaunchAgents/com.googleads.fetcher.plist

launchctl load ~/Library/LaunchAgents/com.googleads.fetcher.plist
```

### Step 3: 登録確認

```bash
launchctl list | grep googleads
```

`com.googleads.fetcher` が表示されれば成功です。

---

## 動作確認テスト

### テスト用トリガーを手動で書き込む

```bash
cat > ~/Desktop/Claude/GoogleAds_Fetcher/fetch_trigger.json << 'EOF'
{
  "action": "fetch",
  "site": "065",
  "from": "2026-01-01",
  "to": "2026-01-07",
  "campaign": "",
  "requested_at": "2026-03-08T00:00:00"
}
EOF
```

数秒後に以下を確認：

```bash
# ステータス確認
cat ~/Desktop/Claude/GoogleAds_Fetcher/fetch_status.json

# ログ確認
tail -20 ~/Desktop/Claude/GoogleAds_Fetcher/fetch_run.log
```

`"status": "done"` が出れば成功です。

---

## アンインストール（必要な場合）

```bash
launchctl unload ~/Library/LaunchAgents/com.googleads.fetcher.plist
rm ~/Library/LaunchAgents/com.googleads.fetcher.plist
```

---

## トラブルシューティング

| 症状 | 確認箇所 |
|------|----------|
| トリガーが反応しない | `launchctl list \| grep googleads` で登録確認 |
| エラーで終了する | `fetch_run.log` / `launchd_err.log` を確認 |
| status が running のまま | `.fetch_lock` ファイルを手動削除して再試行 |
| Mac 再起動後に動かない | `launchctl load` をもう一度実行（または LoginItems に追加） |
