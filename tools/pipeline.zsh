#!/usr/bin/env zsh
# shellcheck shell=sh
# pipeline.zsh — end-to-end: ensure decoded -> resample -> trust-check
# Zsh-native; no bash builtins.

emulate -L zsh
setopt err_return pipe_fail extended_glob null_glob

# --- Paths (relative to repo root) ---
SCRIPT_DIR=${0:A:h}
REPO_ROOT=${SCRIPT_DIR:h}
PAKBUS_DIR="${REPO_ROOT}/biochar_app/pakbus"
OUT_FETCH="${PAKBUS_DIR}/bdFiles/out_fetch"
TOOLS="${REPO_ROOT}/tools"
TRUST_DIR="${OUT_FETCH}/_trust_logs"
mkdir -p "${TRUST_DIR}"

# --- Config ---
FREQ="15min"
MIN_SAMPLES=10
METHODS=("mean" "last" "nearest")
SUFFIX_MAP=(
  mean:"_decoded15mMean.csv"
  last:"_decoded15mLast.csv"
  nearest:"_decoded15mNearest.csv"
)

# Sites/tables to process
SITES=(S1B S1M S1T S2B S2M S2T S3B S3M S3T S4B S4T)
TABLE="Table1"

# Helper: suffix lookup
suffix_for() {
  local m="$1"
  local k v
  for kv in $SUFFIX_MAP; do
    k="${kv%%:*}"; v="${kv##*:}"
    [[ "$k" == "$m" ]] && { print -r -- "$v"; return 0; }
  done
  return 1
}

# --- Step 0: Ensure decoded exists (fallback from collapsed) ---
# If ${site}_${TABLE}_*_decoded.csv is missing but exactly one
# ${site}_${TABLE}_n*_31B_collapsed.csv exists, copy-as decoded.
ensure_decoded() {
  local site="$1" table="$2"
  local decoded_glob="${OUT_FETCH}/${site}_${table}_*_decoded.csv"
  local decoded_files=($decoded_glob (N))
  if (( ${#decoded_files} > 0 )); then
    return 0
  fi

  local collapsed_glob="${OUT_FETCH}/${site}_${table}_n*_31B_collapsed.csv"
  local collapsed_files=($collapsed_glob (N))
  if (( ${#collapsed_files} == 1 )); then
    local src="${collapsed_files[1]}"
    local base="${src:t}"
    # Turn _n450_31B_collapsed.csv -> _decoded.csv
    local dst="${src%_n*}_decoded.csv"
    cp -f -- "$src" "$dst"
    print "[GEN] Created decoded from collapsed for ${site} ${table}: ${dst}"
    return 0
  elif (( ${#collapsed_files} > 1 )); then
    print "[WARN] Multiple collapsed files for ${site} ${table}; skipping auto-decoded."
  else
    print "[SKIP] No decoded or collapsed candidate for ${site} ${table}"
  fi
}

# --- Resampling one decoded CSV in all methods ---
resample_file_all_methods() {
  local src="$1"
  local base="${src:t}"     # filename only

  for method in $METHODS; do
    local suffix="$(suffix_for "$method")"
    # Build method-specific args
    local extra_args=()
    case "$method" in
      mean)     extra_args=(--min-samples "${MIN_SAMPLES}") ;;
      last|nearest) extra_args=() ;;
    esac

    # NOTE: we do not pass --label to avoid earlier warnings.
    python "${TOOLS}/resample_decoded.py" \
      --glob "${src}" \
      --freq "${FREQ}" \
      --method "${method}" \
      --suffix "${suffix}" \
      "${extra_args[@]}"
  done
}

# --- Trust-check a site's collapsed (Last) output against its .dat reference ---
trust_check_leaf() {
  local site="$1" table="$2"
  local dat_path="${REPO_ROOT}/biochar_app/data-raw/datfiles_2025/${site}_${table}.dat"

  if [[ ! -f "${dat_path}" ]]; then
    print "[WARN] Missing .dat for ${site} ${table} → ${dat_path}; skipping trust check."
    return
  fi

  local files=( ${OUT_FETCH}/${site}_${table}_*_decoded15mLast.csv (N) )
  if (( ${#files} == 0 )); then
    print "[SKIP] No collapsed (Last) file for ${site} ${table}"
    return
  fi

  for f in "${files[@]}"; do
    echo "\n[trust] ${f:t}\n"
    "${TOOLS}/trust_check.py" --collapsed "${f}" --dat "${dat_path}" \
      | tee -a "${TRUST_DIR}/${f:t}.log"
  done
}

print "== Standardize (if needed) =="
for s in $SITES; do
  ensure_decoded "$s" "$TABLE"
done

print "\n== Resampling =="
decoded_files=( ${OUT_FETCH}/*_${TABLE}_*_decoded.csv (N) )
if (( ${#decoded_files} == 0 )); then
  # Show per-site SKIPs to help debugging
  for s in $SITES; do
    local pat="${OUT_FETCH}/${s}_${TABLE}_*_decoded.csv"
    local hits=(${~pat} (N) )
    if (( ${#hits} == 0 )); then
      print "[SKIP] No decoded CSV found for ${s} ${TABLE} at ${pat}"
    fi
  done
else
  for src in "${decoded_files[@]}"; do
    resample_file_all_methods "${src}"
  done
fi

print "\n== Trust-checking =="
for s in $SITES; do
  trust_check_leaf "$s" "$TABLE"
done

print "\n== Append to parquet (placeholder) =="
# (placeholder for parquet append step)

print "\nDone."