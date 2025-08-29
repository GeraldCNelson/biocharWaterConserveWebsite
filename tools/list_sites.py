#!/usr/bin/env python3
import sys, yaml, json
from pathlib import Path

cfg = yaml.safe_load(Path("config/sites.yaml").read_text())
for leaf in cfg["leaves"]:
    print(json.dumps({
        "name": leaf["name"],
        "pakbus_id": leaf["pakbus_id"],
        "gateway_host": cfg["gateway"]["host"],
        "router_id": cfg["gateway"]["pakbus_id"],
        "table": cfg["defaults"]["table"],
        "tz": cfg["gateway"].get("timezone","UTC"),
    }))