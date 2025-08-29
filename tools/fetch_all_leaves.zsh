#!/usr/bin/env zsh
set -euo pipefail

leaves_json=("${(@f)$(./tools/list_sites.py)}")
for row in "${leaves_json[@]}"; do
  name=$(echo "$row" | python3 -c 'import sys,json; print(json.load(sys.stdin)["name"])')
  echo "[fetch] $name"
  ./fetch_cr206x_via_gateway.py --site "$name"
done