#!/bin/bash
# ============================================================
# watcher.sh  （~/Library/Scripts/GoogleAds/ に配置すること）
# Mac 上で常駐し、fetch_trigger.json の変化を監視するスクリプト
# ============================================================

FETCHER_DIR="$HOME/Desktop/Claude/GoogleAds_Fetcher"
TRIGGER_FILE="$FETCHER_DIR/fetch_trigger.json"
WATCHER_LOG="$HOME/Library/Scripts/GoogleAds/watcher.log"
RUNNER="$HOME/Library/Scripts/GoogleAds/run_fetcher.sh"

echo "[$(date)] watcher 起動" >> "$WATCHER_LOG"

LAST_REQUESTED_AT=""

while true; do
    if [ -f "$TRIGGER_FILE" ]; then
        CURRENT_REQUESTED_AT=$(python3 -c "
import json, sys
try:
    d = json.load(open('$TRIGGER_FILE'))
    print(d.get('requested_at', ''))
except:
    print('')
" 2>/dev/null)

        if [ -n "$CURRENT_REQUESTED_AT" ] && [ "$CURRENT_REQUESTED_AT" != "$LAST_REQUESTED_AT" ]; then
            echo "[$(date)] 新しいトリガー検知: $CURRENT_REQUESTED_AT" >> "$WATCHER_LOG"
            LAST_REQUESTED_AT="$CURRENT_REQUESTED_AT"
            bash "$RUNNER" &
        fi
    fi
    sleep 5
done
