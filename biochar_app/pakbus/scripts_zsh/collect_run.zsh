#!/usr/bin/env zsh
set -euo pipefail

SESS_DIR="biochar_app/data-raw/spm_sessions"
LOG_DIR="${SESS_DIR}/logs"
mkdir -p "$LOG_DIR"

SPM_FILE="${SESS_DIR}/COM7 Monitoring Session 2025_09_10_14_27.spm"
BASE_NAME="COM7_Monitoring_2025_09_10_14_27"

# Step 1: Run spm_list_frames and keep CollectData lines in logs
COLLECT_LOG="${LOG_DIR}/${BASE_NAME}.collect.txt"
echo "=== Step 1: Extract CollectData lines from ${SPM_FILE}"
python -m biochar_app.pakbus.spm_list_frames "$SPM_FILE" \
  | grep -E 'CollectData (req|resp)' \
  > "$COLLECT_LOG"

lines=$(wc -l < "$COLLECT_LOG" | tr -d ' ')
echo "[ok] Wrote $lines CollectData lines → $COLLECT_LOG"

# Step 2: Try to parse them as binary frames
echo
echo "=== Step 2: Dump parsed CollectData payloads (tail 80) ==="
python -m biochar_app.pakbus.spm_collectdata_dump "$SPM_FILE" --tail 80 \
  || echo "[warn] spm_collectdata_dump could not parse CollectData from $SPM_FILE"