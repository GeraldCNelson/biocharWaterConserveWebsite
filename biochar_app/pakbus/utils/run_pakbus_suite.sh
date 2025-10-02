#!/usr/bin/env bash
set -Eeuo pipefail

ADDR="2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd"
PORT=6785
TIMEOUT=20
OUTDIR="$(pwd)/pakbus_suite_$(date +%Y%m%d_%H%M%S)"
COMMON_BASE=(python -m biochar_app.pakbus.scripts.fetch_table1_live \
  --addr "$ADDR" --port "$PORT" --timeout "$TIMEOUT" --debug --router-outermost --always-dump-raw)

mkdir -p "$OUTDIR"
echo "[INFO] Log dir: $OUTDIR"
echo "[INFO] Working dir: $(pwd)"
echo "[INFO] Python: $(command -v python)"
python --version || true

run_case () {
  local name="$1"; shift
  local log="$OUTDIR/${name}.log"
  echo
  echo "==== RUN: $name ($(date -Is)) ===="
  echo "[LOG] $log"
  printf "[CMD] %q " "$@" | tee "$log"
  echo | tee -a "$log"
  # Bash 3.2: no |&, so manually merge stderr->stdout before tee
  ( "$@" ) > >(tee -a "$log") 2> >(tee -a "$log" >&2) || {
    echo "[WARN] case '$name' exited nonzero" | tee -a "$log"
  }
}

# 1) probe leaf 3 (tran=0x90)
run_case "probe_leaf3_tran90" \
  "${COMMON_BASE[@]}" \
  --pre-hex "A0 01 6F FD 10 00 30 FF D0 90" \
  --pre-wait-ms 600 \
  --tran 0x90 \
  --probe-first \
  --leaf 3

# 2) last record from table 1, leaf 3
run_case "lastrec_tbl1_leaf3_tran90" \
  "${COMMON_BASE[@]}" \
  --pre-hex "A0 01 6F FD 10 00 30 FF D0 90" \
  --pre-wait-ms 600 \
  --tran 0x90 \
  --leaf 3 \
  --opcode 0x00 \
  --table-id 0x0001 \
  --start-rec 0xFFFF \
  --count 0x0001

# 3) PBRouter-header variant
run_case "lastrec_tbl1_leaf3_forcedhdr" \
  "${COMMON_BASE[@]}" \
  --pre-hex "A0 01 6F FD 10 00 30 FF D0 90" \
  --pre-wait-ms 600 \
  --tran 0x90 \
  --mirror-header-from-pre \
  --mirror-header-len 10 \
  --tran-idx 9 \
  --prefix-hex "0D 00 00 50 00 02" \
  --prefix-pos post_router \
  --leaf 3 \
  --opcode 0x00 \
  --table-id 0x0001 \
  --start-rec 0xFFFF \
  --count 0x0001

# 4) sweep leaves 2–13
for L in {2..13}; do
  run_case "sweep_leaf${L}_lastrec_tran90" \
    "${COMMON_BASE[@]}" \
    --pre-hex "A0 01 6F FD 10 00 30 FF D0 90" \
    --pre-wait-ms 600 \
    --tran 0x90 \
    --leaf "$L" \
    --opcode 0x00 \
    --table-id 0x0001 \
    --start-rec 0xFFFF \
    --count 0x0001
done

echo
echo "[DONE] All tests launched. Logs in: $OUTDIR"