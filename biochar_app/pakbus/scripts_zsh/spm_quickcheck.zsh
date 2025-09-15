#!/usr/bin/env zsh
# spm_quickcheck.zsh — quick schema + CollectData sanity check
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/session.spm"
  exit 1
fi

SPM_FILE="$1"
echo "=== Checking SPM session: $SPM_FILE ==="

# Step 1: List all frames (skim for CollectData req/resp lines)
echo
echo "--- Frames ---"
python -m biochar_app.pakbus.spm_list_frames "$SPM_FILE" | tee "${SPM_FILE%.spm}.frames.txt"

# Step 2: Extract schema (table definitions)
echo
echo "--- TableDefs ---"
python -m biochar_app.pakbus.spm_extract_tabledefs "$SPM_FILE" | tee "${SPM_FILE%.spm}.tabledefs.txt"

# Step 3: Parse CollectData payloads (full dump, tail for recent rows)
echo
echo "--- CollectData ---"
python -m biochar_app.pakbus.spm_collectdata_dump "$SPM_FILE" --tail 50 | tee "${SPM_FILE%.spm}.collect.txt"

echo
echo "[ok] Outputs written: *.frames.txt, *.tabledefs.txt, *.collect.txt alongside the SPM file"