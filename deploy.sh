#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# Biochar test -> production deploy helper
# ============================================================
# What this script does:
# 1. Verifies local repo exists
# 2. Optionally checks git branch / cleanliness
# 3. Rsyncs large data directories to production using
#    absolute remote paths (avoids literal "~" bug)
# 4. Verifies required parquet files exist on production
# 5. Optionally regenerates derived files on production
# 6. Restarts the biochar service on production
#
# Usage examples:
#   ./deploy.sh
#   ./deploy.sh --no-restart - nothing changed, just tested
#   ./deploy.sh --skip-rsync
#   ./deploy.sh --regen
#   ./deploy.sh --year 2026
# ============================================================

# ----------------------------
# Config
# ----------------------------
LOCAL_REPO="$(cd "$(dirname "$0")" && pwd)"
REMOTE_HOST="biochar-webserver"
REMOTE_REPO="/home/ubuntu/biocharWaterConserveWebsite"

LOCAL_PARQUET_DIR="${LOCAL_REPO}/biochar_app/data-processed/parquet"
LOCAL_DOWNLOADS_DIR="${LOCAL_REPO}/biochar_app/data-processed/downloads"

REMOTE_PARQUET_DIR="${REMOTE_REPO}/biochar_app/data-processed/parquet"
REMOTE_DOWNLOADS_DIR="${REMOTE_REPO}/biochar_app/data-processed/downloads"

REMOTE_VENV="${REMOTE_REPO}/venv/bin/activate"
REMOTE_SERVICE="biochar"

DEFAULT_YEAR="2025"
CHECK_GRANULARITIES=("daily" "hourly" "monthly" "15min")

# ----------------------------
# Flags
# ----------------------------
DO_RSYNC=1
DO_RESTART=1
DO_REGEN=0
CHECK_GIT=1
YEAR="${DEFAULT_YEAR}"

# ----------------------------
# Helpers
# ----------------------------
log() {
  printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ./deploy.sh [options]

Options:
  --year YYYY       Year to verify on production (default: 2025)
  --skip-rsync      Do not rsync parquet/download directories
  --no-restart      Do not restart production service
  --regen           Regenerate converted files on production
  --no-git-check    Skip local git branch / status checks
  -h, --help        Show this help

Examples:
  ./deploy.sh
  ./deploy.sh --year 2026
  ./deploy.sh --regen
  ./deploy.sh --skip-rsync --no-restart
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

run_remote() {
  ssh "${REMOTE_HOST}" "$@"
}

verify_local_paths() {
  [[ -d "${LOCAL_REPO}" ]] || die "Local repo not found: ${LOCAL_REPO}"
  [[ -d "${LOCAL_PARQUET_DIR}" ]] || die "Local parquet dir not found: ${LOCAL_PARQUET_DIR}"
  [[ -d "${LOCAL_DOWNLOADS_DIR}" ]] || die "Local downloads dir not found: ${LOCAL_DOWNLOADS_DIR}"
}

check_local_git() {
  [[ "${CHECK_GIT}" -eq 1 ]] || return 0

  log "Checking local git state..."
  cd "${LOCAL_REPO}"

  local branch
  branch="$(git branch --show-current)"
  printf 'Current branch: %s\n' "${branch}"

  if [[ "${branch}" != "main" && "${branch}" != "etl-refactor" ]]; then
    printf 'Warning: you are on branch "%s"\n' "${branch}"
  fi

  if ! git diff --quiet || ! git diff --cached --quiet; then
    printf 'Warning: local repo has uncommitted changes.\n'
    git status --short
  fi
}

sync_data() {
  [[ "${DO_RSYNC}" -eq 1 ]] || return 0

  log "Syncing parquet to production..."
  rsync -av  --exclude '.DS_Store'\
    "${LOCAL_PARQUET_DIR}/" \
    "${REMOTE_HOST}:${REMOTE_PARQUET_DIR}/"

  log "Syncing downloads to production..."
  rsync -av  --exclude '.DS_Store'\
    "${LOCAL_DOWNLOADS_DIR}/" \
    "${REMOTE_HOST}:${REMOTE_DOWNLOADS_DIR}/"
}

check_for_bad_tilde_dir() {
  log "Checking for stray literal ~ directory on production..."
  run_remote "
    if [[ -d '${REMOTE_PARQUET_DIR}/~' ]]; then
      echo 'WARNING: Found bad directory: ${REMOTE_PARQUET_DIR}/~'
      echo 'Rename it manually if needed:'
      echo '  mv ${REMOTE_PARQUET_DIR}/~ ${REMOTE_PARQUET_DIR}/_bad_tilde_backup'
      exit 2
    else
      echo 'No bad ~ directory found.'
    fi
  "
}

verify_remote_files() {
  log "Verifying required parquet files on production for year ${YEAR}..."

  local missing=0

  for gran in "${CHECK_GRANULARITIES[@]}"; do
    local remote_file
    if [[ "${gran}" == "15min" ]]; then
      remote_file="${REMOTE_PARQUET_DIR}/summary/15min/${YEAR}_15min.parquet"
    else
      remote_file="${REMOTE_PARQUET_DIR}/summary/${gran}/${YEAR}_${gran}.parquet"
    fi

    if run_remote "[[ -f '${remote_file}' ]]"; then
      printf 'OK: %s\n' "${remote_file}"
    else
      printf 'MISSING: %s\n' "${remote_file}"
      missing=1
    fi
  done

  [[ "${missing}" -eq 0 ]] || die "One or more required parquet files are missing on production."
}

regen_remote_files() {
  [[ "${DO_REGEN}" -eq 1 ]] || return 0

  log "Regenerating derived files on production..."
  run_remote "
    set -e
    cd '${REMOTE_REPO}'
    source '${REMOTE_VENV}'
    python biochar_app/scripts/convert_word_to_html.py
    python biochar_app/scripts/convert_ward_docx_to_html.py
  "
}

restart_remote_service() {
  [[ "${DO_RESTART}" -eq 1 ]] || return 0

  log "Restarting production service..."
  run_remote "
    sudo systemctl daemon-reload
    sudo systemctl restart '${REMOTE_SERVICE}'
    sudo systemctl --no-pager --full status '${REMOTE_SERVICE}'
  "
}

print_post_checks() {
  cat <<EOF

Deployment finished.

Recommended manual checks:
  1. Open https://biocharresearch.org/
  2. Hard refresh (Cmd + Shift + R)
  3. Verify:
     - plots load
     - summary tab works
     - Ward images render
     - glossary tab works
     - downloads work

Useful troubleshooting commands:
  ssh ${REMOTE_HOST}
  sudo journalctl -u ${REMOTE_SERVICE} -n 100 --no-pager

EOF
}

# ----------------------------
# Parse args
# ----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --year)
      shift
      [[ $# -gt 0 ]] || die "--year requires a value"
      YEAR="$1"
      ;;
    --skip-rsync)
      DO_RSYNC=0
      ;;
    --no-restart)
      DO_RESTART=0
      ;;
    --regen)
      DO_REGEN=1
      ;;
    --no-git-check)
      CHECK_GIT=0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
  shift
done

# ----------------------------
# Main
# ----------------------------
require_cmd git
require_cmd rsync
require_cmd ssh

verify_local_paths
check_local_git
sync_data
check_for_bad_tilde_dir
verify_remote_files
regen_remote_files
restart_remote_service
print_post_checks