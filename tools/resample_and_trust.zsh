#!/usr/bin/env zsh
# Resample all *_decoded.csv three ways and trust-check them,
# then write a one-line summary per file to _trust_summary.csv

set -euo pipefail
setopt null_glob extended_glob

OUTDIR="biochar_app/pakbus/bdFiles/out_fetch"
DATDIR="biochar_app/data-raw/datfiles_2025"
SUMMARY="${OUTDIR}/_trust_summary.csv"

print "== Resampling =="

# Mean of numeric columns in 15-min windows; require >=10 raw samples
python tools/resample_decoded.py \
  --glob "${OUTDIR}/*_decoded.csv" \
  --freq 15min \
  --min-samples 10 \
  --method mean \
  --suffix _decoded15mMean.csv

# Last/nearest-to-anchor per 15-min window
python tools/resample_decoded.py \
  --glob "${OUTDIR}/*_decoded.csv" \
  --freq 15min \
  --method last \
  --suffix _decoded15mLast.csv

python tools/resample_decoded.py \
  --glob "${OUTDIR}/*_decoded.csv" \
  --freq 15min \
  --method nearest \
  --max-gap 8min \
  --suffix _decoded15mNearest.csv

print "\n== Trust-checking ==\n"

# Fresh summary header
echo "station,method,overlap_rows,max_abs_delta,outliers_gt_0.005" > "$SUMMARY"

# Helper: parse one trust_check.py log and append a CSV row
parse_and_append() {
  local log="$1" base="$2" method="$3"

  # overlap rows
  local overlap
  overlap="$(awk -F':' '/^overlap rows/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}' "$log")"
  [[ -z "$overlap" ]] && overlap="0"

  # max |Δ| across the per-field block
  # (grab numbers at end of lines like "   BattV_Min: 0.1234")
  local maxdelta
  maxdelta="$(
    awk '
      /^-- Max \|Δ\| by field --/ { inblock=1; next }
      inblock && NF==0 { inblock=0 }
      inblock && $0 ~ /^[[:space:]]*[A-Za-z0-9_]+:[[:space:]]/ {
        v=$NF+0; if (v>m) m=v
      }
      END { if (m=="") print "0"; else printf("%.6f\n", m) }
    ' "$log"
  )"

  # outliers count (or 0 if the "No per-field deltas..." line appears)
  local outliers
  outliers="$(sed -n 's/^-- Outliers > 0\.005 (\([0-9][0-9]*\) rows) --/\1/p' "$log")"
  if [[ -z "$outliers" ]]; then
    if grep -q "^No per-field deltas exceeded 0\.005" "$log"; then
      outliers="0"
    else
      # Fallback if format changes — default to 0
      outliers="0"
    fi
  fi

  echo "${base},${method},${overlap},${maxdelta},${outliers}" >> "$SUMMARY"
}

# Run trust_check over each resampled file and append to summary
for f in ${OUTDIR}/*_decoded15m*.csv; do
  bn="${f:t}"                # basename
  base="${bn%%_*}"           # e.g., S3B
  method="${bn#*_decoded}"   # e.g., 15mMean.csv
  method="${method%.csv}"    # e.g., 15mMean

  print "\n[trust] ${bn}\n"
  tmp=$(mktemp)
  ./tools/trust_check.py --collapsed "$f" --dat "${DATDIR}/${base}_Table1.dat" | tee "$tmp"
  parse_and_append "$tmp" "$base" "$method"
  rm -f "$tmp"
done

print "\n== Summary =="
print "Saved: $SUMMARY\n"
# Pretty-print the CSV for quick inspection
command -v column >/dev/null && column -s, -t "$SUMMARY" || cat "$SUMMARY"