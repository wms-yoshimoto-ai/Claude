#!/bin/bash
# ============================================================
# run_fetcher.sh
# launchd の WatchPaths によって自動起動されるラッパースクリプト
# Cowork が fetch_trigger.json に指示を書くと、このスクリプトが実行される
# ============================================================

SCRIPT_DIR="$HOME/Desktop/Claude/GoogleAds_Fetcher"
DATA_DIR="$HOME/Documents/GoogleAds_Data"
TRIGGER_FILE="$SCRIPT_DIR/fetch_trigger.json"
STATUS_FILE="$SCRIPT_DIR/fetch_status.json"
LOG_FILE="$SCRIPT_DIR/fetch_run.log"

# ロックファイルで二重起動を防止
LOCK_FILE="$SCRIPT_DIR/.fetch_lock"
if [ -f "$LOCK_FILE" ]; then
    echo "[$(date)] すでに実行中です (lock file exists)" >> "$LOG_FILE"
    exit 0
fi
touch "$LOCK_FILE"
trap "rm -f '$LOCK_FILE'" EXIT

echo "================================================" >> "$LOG_FILE"
echo "[$(date)] fetch_trigger.json を検知、実行開始" >> "$LOG_FILE"

# ステータスを "running" に更新
python3 - <<'PYEOF'
import json, sys
from datetime import datetime
from pathlib import Path
import os
status_file = Path(os.environ.get('HOME')) / "Desktop/Claude/GoogleAds_Fetcher/fetch_status.json"
with open(status_file, "w") as f:
    json.dump({"status": "running", "started_at": datetime.now().isoformat(), "message": "実行中..."}, f, ensure_ascii=False)
PYEOF

# トリガーファイルを読み込む
if [ ! -f "$TRIGGER_FILE" ]; then
    echo "[$(date)] エラー: fetch_trigger.json が見つかりません" >> "$LOG_FILE"
    python3 - <<'PYEOF'
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / "Desktop/Claude/GoogleAds_Fetcher/fetch_status.json"
with open(status_file, "w") as f:
    json.dump({"status": "error", "message": "fetch_trigger.json が見つかりません", "finished_at": datetime.now().isoformat()}, f, ensure_ascii=False)
PYEOF
    exit 1
fi

# トリガーファイルをパース
eval $(python3 - <<PYEOF
import json, sys
try:
    d = json.load(open("$TRIGGER_FILE"))
    print(f"ACTION={d.get('action','fetch')}")
    print(f"SITE={d.get('site','')}")
    print(f"DATE_FROM={d.get('from','')}")
    print(f"DATE_TO={d.get('to','')}")
    print(f"CAMPAIGN={d.get('campaign','')}")
    print(f"WITH_LIST_KEYWORDS={'1' if d.get('with_list_keywords') else ''}")
except Exception as e:
    print(f"PARSE_ERROR={e}")
PYEOF
)

echo "[$(date)] action=$ACTION site=$SITE from=$DATE_FROM to=$DATE_TO campaign=$CAMPAIGN with_list_keywords=$WITH_LIST_KEYWORDS" >> "$LOG_FILE"

# ============================================================
# アクションに応じて実行
# ============================================================
mkdir -p "$DATA_DIR"

# ── git_push: CoworkからGitHubへのコード反映 ──────────────
if [ "$ACTION" = "git_push" ]; then
    COMMIT_MSG=$(python3 -c "
import json
try:
    d = json.load(open('$TRIGGER_FILE'))
    print(d.get('message', 'Coworkからの自動コミット'))
except:
    print('Coworkからの自動コミット')
")
    echo "[$(date)] git push 開始: $COMMIT_MSG" >> "$LOG_FILE"
    cd "$SCRIPT_DIR"
    git add -A >> "$LOG_FILE" 2>&1
    git commit -m "$COMMIT_MSG" >> "$LOG_FILE" 2>&1
    GIT_EXIT=$?
    if [ $GIT_EXIT -eq 0 ] || [ $GIT_EXIT -eq 1 ]; then
        # exit 1 = "nothing to commit" も正常扱い
        git pull --rebase >> "$LOG_FILE" 2>&1
        git push >> "$LOG_FILE" 2>&1
        PUSH_EXIT=$?
    fi
    # run_fetcher.sh を Library にコピー（常に最新版を反映）
    LIB_DIR="$HOME/Library/Scripts/GoogleAds"
    mkdir -p "$LIB_DIR"
    cp "$SCRIPT_DIR/run_fetcher.sh" "$LIB_DIR/run_fetcher.sh"
    chmod +x "$LIB_DIR/run_fetcher.sh"
    echo "[$(date)] run_fetcher.sh → $LIB_DIR/ コピー完了" >> "$LOG_FILE"
    python3 -c "
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / 'Desktop/Claude/GoogleAds_Fetcher/fetch_status.json'
with open(status_file, 'w') as f:
    json.dump({
        'status': 'done',
        'action': 'git_push',
        'message': 'GitHubへのプッシュ完了',
        'finished_at': datetime.now().isoformat()
    }, f, ensure_ascii=False, indent=2)
"
    exit 0
fi

# ── git_pull: GitHubからMacへ最新コードを取得 ─────────────
if [ "$ACTION" = "git_pull" ]; then
    echo "[$(date)] git pull 開始" >> "$LOG_FILE"
    cd "$SCRIPT_DIR"
    git pull >> "$LOG_FILE" 2>&1
    python3 -c "
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / 'Desktop/Claude/GoogleAds_Fetcher/fetch_status.json'
with open(status_file, 'w') as f:
    json.dump({
        'status': 'done',
        'action': 'git_pull',
        'message': 'GitHubから最新コードを取得しました',
        'finished_at': datetime.now().isoformat()
    }, f, ensure_ascii=False, indent=2)
"
    exit 0
fi

# ── add_campaign: キャンペーン追加 ────────────────────────
if [ "$ACTION" = "add_campaign" ]; then
    echo "[$(date)] add_campaign 開始" >> "$LOG_FILE"
    python3 - <<PYEOF >> "$LOG_FILE" 2>&1
import json, sys, os
from pathlib import Path
sys.path.insert(0, os.environ['HOME'] + '/Desktop/Claude/GoogleAds_Fetcher')
from campaign_db import add_campaign
d = json.load(open("$TRIGGER_FILE"))
add_campaign(
    site_id=d.get('site_id', d.get('site', '')),
    campaign_id=str(d.get('campaign_id', '')),
    campaign_name=d.get('campaign_name', ''),
    campaign_type=d.get('campaign_type', '検索')
)
PYEOF
    EXIT=$?
    python3 -c "
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / 'Desktop/Claude/GoogleAds_Fetcher/fetch_status.json'
with open(status_file, 'w') as f:
    json.dump({'status': 'done' if $EXIT == 0 else 'error', 'action': 'add_campaign', 'message': 'キャンペーン追加完了' if $EXIT == 0 else 'エラーが発生しました', 'finished_at': datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
"
    exit $EXIT
fi

# ── rename_campaign: キャンペーン名変更 ───────────────────
if [ "$ACTION" = "rename_campaign" ]; then
    echo "[$(date)] rename_campaign 開始" >> "$LOG_FILE"
    python3 - <<PYEOF >> "$LOG_FILE" 2>&1
import json, sys, os
from pathlib import Path
sys.path.insert(0, os.environ['HOME'] + '/Desktop/Claude/GoogleAds_Fetcher')
from campaign_db import rename_campaign
d = json.load(open("$TRIGGER_FILE"))
rename_campaign(
    campaign_id=str(d.get('campaign_id', '')),
    new_name=d.get('new_name', '')
)
PYEOF
    EXIT=$?
    python3 -c "
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / 'Desktop/Claude/GoogleAds_Fetcher/fetch_status.json'
with open(status_file, 'w') as f:
    json.dump({'status': 'done' if $EXIT == 0 else 'error', 'action': 'rename_campaign', 'message': 'キャンペーン名変更完了' if $EXIT == 0 else 'エラーが発生しました', 'finished_at': datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
"
    exit $EXIT
fi

# ── データ取得系：バリデーション ─────────────────────────
# fetch_campaign_settings / fetch_change_history は日付不要（省略可）なためバリデーション対象外
if [ "$ACTION" != "fetch_campaign_settings" ] && [ "$ACTION" != "fetch_change_history" ] && [ "$ACTION" != "fetch_negative_keyword" ] && [ "$ACTION" != "fetch_assets" ] && [ "$ACTION" != "fetch_asset_report" ] && [ "$ACTION" != "fetch_audience_settings" ] && { [ -z "$SITE" ] || [ -z "$DATE_FROM" ] || [ -z "$DATE_TO" ]; }; then
    MSG="エラー: site / from / to が指定されていません"
    echo "[$(date)] $MSG" >> "$LOG_FILE"
    python3 -c "
import json, os
from datetime import datetime
from pathlib import Path
status_file = Path(os.environ.get('HOME')) / 'Desktop/Claude/GoogleAds_Fetcher/fetch_status.json'
with open(status_file, 'w') as f:
    json.dump({'status': 'error', 'message': '$MSG', 'finished_at': datetime.now().isoformat()}, f, ensure_ascii=False)
"
    exit 1
fi

if [ "$ACTION" = "fetch_location" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_user_location.py --site $SITE --from $DATE_FROM --to $DATE_TO --export-csv"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_keyword" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_keyword.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_search_term" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_search_term.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_campaign_daily" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_campaign_daily.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_ad_group_daily" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_ad_group_daily.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_ad_daily" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_ad_daily.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_asset_group" ]; then
    AG_MODE=$(python3 -c "
import json
try:
    d = json.load(open('$TRIGGER_FILE'))
    print(d.get('mode', 'period'))
except:
    print('period')
")
    CMD="python3 $SCRIPT_DIR/fetch_asset_group.py --site $SITE --from $DATE_FROM --to $DATE_TO --mode $AG_MODE"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_campaign_settings" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_campaign_settings.py --site $SITE"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_change_history" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_change_history.py --site $SITE"
    if [ -n "$DATE_FROM" ]; then
        CMD="$CMD --from $DATE_FROM"
    fi
    if [ -n "$DATE_TO" ]; then
        CMD="$CMD --to $DATE_TO"
    fi
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_negative_keyword" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_negative_keyword.py --site $SITE"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
    if [ -n "$WITH_LIST_KEYWORDS" ]; then
        CMD="$CMD --with-list-keywords"
    fi
elif [ "$ACTION" = "fetch_assets" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_assets.py --site $SITE"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_audience_settings" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_audience_settings.py --site $SITE"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_asset_report" ]; then
    # アセットの関連付けレポート（期間集計・level 指定可）
    LEVEL=$(python3 -c "
import json
try:
    d = json.load(open('$TRIGGER_FILE'))
    print(d.get('level', 'all'))
except:
    print('all')
")
    if [ -n "$DATE_FROM" ] && [ -n "$DATE_TO" ]; then
        CMD="python3 $SCRIPT_DIR/fetch_asset_report.py --site $SITE --from $DATE_FROM --to $DATE_TO --level $LEVEL"
    else
        # month パラメータが渡された場合
        MONTH=$(python3 -c "
import json
try:
    d = json.load(open('$TRIGGER_FILE'))
    print(d.get('month', ''))
except:
    print('')
")
        CMD="python3 $SCRIPT_DIR/fetch_asset_report.py --site $SITE --month $MONTH --level $LEVEL"
    fi
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "fetch_placement" ]; then
    SUMMARY_ONLY=$(python3 -c "
import json
try:
    d = json.load(open('$TRIGGER_FILE'))
    print('1' if d.get('summary_only') else '')
except:
    print('')
")
    CMD="python3 $SCRIPT_DIR/fetch_placement.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
    if [ -n "$SUMMARY_ONLY" ]; then
        CMD="$CMD --summary-only"
    fi
elif [ "$ACTION" = "fetch_auction_insight" ]; then
    CMD="python3 $SCRIPT_DIR/fetch_auction_insight.py --site $SITE --from $DATE_FROM --to $DATE_TO"
    if [ -n "$CAMPAIGN" ]; then
        CMD="$CMD --campaign $CAMPAIGN"
    fi
elif [ "$ACTION" = "test_pmax_st" ]; then
    CMD="python3 $SCRIPT_DIR/test_pmax_search_term.py"
else
    CMD="python3 $SCRIPT_DIR/fetch_google_ads.py --account $SITE --from $DATE_FROM --to $DATE_TO"
fi

echo "[$(date)] 実行コマンド: $CMD" >> "$LOG_FILE"
eval $CMD >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# ============================================================
# 完了ステータスを書き込む
# ============================================================
python3 - <<PYEOF
import json, os
from datetime import datetime
from pathlib import Path

status_file = Path(os.environ.get('HOME')) / "Desktop/Claude/GoogleAds_Fetcher/fetch_status.json"
exit_code = $EXIT_CODE
action = "$ACTION"
site = "$SITE"
date_from = "$DATE_FROM"
date_to = "$DATE_TO"

if exit_code == 0:
    # 出力ファイルを特定
    data_dir = Path(os.environ.get('HOME')) / "Documents/GoogleAds_Data"
    if action == "fetch_location":
        output_file = str(data_dir / f"{site}_user_location_{date_from}_{date_to}.json")
    elif action == "fetch_keyword":
        output_file = str(data_dir / f"{site}_keyword_{date_from}_{date_to}.json")
    elif action == "fetch_search_term":
        output_file = str(data_dir / f"{site}_search_term_{date_from}_{date_to}.json")
    elif action == "fetch_campaign_daily":
        output_file = str(data_dir / f"{site}_campaign_daily_{date_from}_{date_to}.json")
    elif action == "fetch_ad_group_daily":
        output_file = str(data_dir / f"{site}_ad_group_daily_{date_from}_{date_to}.json")
    elif action == "fetch_ad_daily":
        output_file = str(data_dir / f"{site}_ad_daily_{date_from}_{date_to}.json")
    elif action == "fetch_asset_group":
        import json as _json2
        try:
            _d2 = _json2.load(open(os.environ.get('HOME') + '/Desktop/Claude/GoogleAds_Fetcher/fetch_trigger.json'))
            _ag_mode = _d2.get('mode', 'period')
        except:
            _ag_mode = 'period'
        _mode_suffix = '' if _ag_mode == 'period' else f'_{_ag_mode}'
        output_file = str(data_dir / f"{site}_asset_group{_mode_suffix}_{date_from}_{date_to}.json")
    elif action == "fetch_campaign_settings":
        from datetime import datetime as _dt
        output_file = str(data_dir / f"{site}_campaign_settings_{_dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    elif action == "fetch_change_history":
        from datetime import datetime as _dt
        output_file = str(data_dir / f"{site}_change_history_{_dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    elif action == "fetch_negative_keyword":
        from datetime import datetime as _dt
        output_file = str(data_dir / f"{site}_negative_keyword_{_dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    elif action == "fetch_assets":
        # fetch_assets.py は実行開始時のタイムスタンプでファイルを作成するため、
        # 完了時にglobで最新ファイルを探す
        import glob as _glob
        _candidates = sorted(_glob.glob(str(data_dir / f"{site}_assets_*.json")))
        output_file = _candidates[-1] if _candidates else str(data_dir / f"{site}_assets_unknown.json")
    elif action == "fetch_audience_settings":
        import glob as _glob
        _candidates = sorted(_glob.glob(str(data_dir / f"{site}_audience_settings_*.json")))
        output_file = _candidates[-1] if _candidates else str(data_dir / f"{site}_audience_settings_unknown.json")
    elif action == "fetch_asset_report":
        import json as _json
        _d = _json.load(open(os.environ.get('HOME') + '/Desktop/Claude/GoogleAds_Fetcher/fetch_trigger.json'))
        _level = _d.get('level', 'all')
        output_file = str(data_dir / f"{site}_asset_report_{_level}_{date_from}_{date_to}.json")
    elif action == "fetch_placement":
        output_file = str(data_dir / f"{site}_placement_{date_from}_{date_to}.json")
    elif action == "fetch_auction_insight":
        output_file = str(data_dir / f"{site}_auction_insight_{date_from}_{date_to}.json")
    else:
        # customer_id から site_id のマッピング
        accounts_file = Path(os.environ.get('HOME')) / "Desktop/Claude/GoogleAds_Fetcher/config/accounts.json"
        accounts = json.load(open(accounts_file))["accounts"]
        acct = next((a for a in accounts if a.get("site_id") == site), None)
        cid = acct["customer_id"].replace("-","") if acct else site
        output_file = str(data_dir / f"{cid}_weekly.json")

    with open(status_file, "w") as f:
        json.dump({
            "status": "done",
            "action": action,
            "site": site,
            "period": {"from": date_from, "to": date_to},
            "output_file": output_file,
            "finished_at": datetime.now().isoformat(),
            "message": "取得完了"
        }, f, ensure_ascii=False, indent=2)
else:
    with open(status_file, "w") as f:
        json.dump({
            "status": "error",
            "exit_code": exit_code,
            "finished_at": datetime.now().isoformat(),
            "message": f"スクリプトがエラーで終了しました (exit code: {exit_code})"
        }, f, ensure_ascii=False, indent=2)
PYEOF

echo "[$(date)] 完了 (exit=$EXIT_CODE)" >> "$LOG_FILE"
exit $EXIT_CODE
