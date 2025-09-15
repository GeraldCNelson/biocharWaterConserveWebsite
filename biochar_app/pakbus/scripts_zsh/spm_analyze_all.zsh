#!/usr/bin/env zsh
set -euo pipefail

# Directories
SESS_DIR="biochar_app/data-raw/spm_sessions"
LOG_DIR="${SESS_DIR}/logs"
mkdir -p "$LOG_DIR"

# Helper (single entry point for list/extract/dump; handles .spm and .txt)
PYHELPER="biochar_app/pakbus/spm_pyhelper.py"

# Timestamp (BSD/macOS date format)
ts() { date "+%Y%m%d-%H%M%S"; }

# Gather inputs: accept both .spm and .txt; (N) => null_glob (skip if none)
typeset -a INPUTS
INPUTS=("$SESS_DIR"/*.spm(N) "$SESS_DIR"/*.txt(N))

if (( ${#INPUTS} == 0 )); then
  echo "No .spm or .txt files in $SESS_DIR"
  exit 0
fi

for spm in "${INPUTS[@]}"; do
  # Make a safe base name for logs (strip path + extension, replace spaces with _)
  base="${spm:t:r}"
  safe_base="${base// /_}"
  log="${LOG_DIR}/${safe_base}.analysis.$(ts).log"

  {
    echo
    echo "==== Analyzing: $spm ===="
    echo "Log: $log"
    echo "Timestamp: $(ts)"

    echo "---- spm_list_frames ----"
    python "$PYHELPER" list-frames "$spm" || echo "[warn] list-frames exited non-zero"
    echo

    echo "---- spm_extract_tabledefs ----"
    python "$PYHELPER" extract-tabledefs "$spm" || echo "[warn] extract-tabledefs exited non-zero"
    echo

    echo "---- spm_collectdata_dump (tail 30) ----"
    # Show only the last 30 lines of the dump for readability
    python "$PYHELPER" collectdata-dump "$spm" | tail -n 30 || echo "[warn] collectdata-dump exited non-zero"
    echo

    echo "Done: $(ts)"
  } | tee "$log"
done

echo
echo "[ok] Logs written to: $LOG_DIR"