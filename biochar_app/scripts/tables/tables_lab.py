# biochar_app/scripts/tables_lab.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from biochar_app.config import table_specs


# -----------------------------
# Normalization helpers
# -----------------------------

def normalize_strip(value: Any) -> str:
    """
    Normalize strip variants to 'STRIP 1'..'STRIP 4'.

    Examples:
      'strip_1' -> 'STRIP 1'
      'STRIP 1' -> 'STRIP 1'
      'S1'      -> 'STRIP 1'
    """
    if value is None:
        return ""
    s = str(value).strip().upper().replace("-", " ").replace("_", " ")
    s = " ".join(s.split())
    if not s:
        return ""

    if s.startswith("STRIP "):
        return s

    if s in {"S1", "S2", "S3", "S4"}:
        return f"STRIP {s[1]}"

    # "STRIP1"
    if s.startswith("STRIP") and len(s) >= 6 and s[5].isdigit():
        return f"STRIP {s[5]}"

    # "STRIP  1" or "STRIP-1" etc.
    if s.startswith("STRIP") and any(ch.isdigit() for ch in s):
        digit = next((ch for ch in s if ch.isdigit()), "")
        return f"STRIP {digit}" if digit else s

    return s


def coerce_date_to_iso(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def choose_first_present(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    colset = {c for c in columns}
    for c in candidates:
        if c in colset:
            return c
    return None


# -----------------------------
# Payload shape used by frontend tables.js
# -----------------------------

def _payload_template(label: str, note: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "label": label,          # e.g. "Set 1: Pasture Quality Metrics"
        "periods": [],           # list[str] (column headers)
        "rows": [],              # list[str] (row keys)
        "rowLabels": {},         # rowKey -> human label
        "data": {},              # variableKey -> {rowKey -> {period -> value}}
    }
    if note:
        payload["note"] = note
    return payload


# -----------------------------
# Core builders
# -----------------------------

def build_lab_table_payload_long(
    df: pd.DataFrame,
    *,
    set_label: str,
    set_note: str,
    row_key: str,
    period_key: str,
    variable_specs: Sequence[Any],  # dicts: {"key","label","candidates"}
    normalize_row_as_strip: bool = False,
) -> Dict[str, Any]:
    """
    Convert a LONG-ish lab dataset into the wide-table payload:
      rows => entity (strip or location)
      columns => sampling event dates (ISO)
      values => each variable
    """
    out = _payload_template(set_label, set_note)

    if row_key not in df.columns or period_key not in df.columns:
        raise ValueError(f"Missing required columns: row_key='{row_key}' period_key='{period_key}'")

    tmp = df.copy()

    # Normalize row ids if row_key is strip-based
    if normalize_row_as_strip:
        tmp[row_key] = tmp[row_key].map(normalize_strip)
    else:
        tmp[row_key] = tmp[row_key].astype(str)

    # ISO event code
    tmp["_event"] = coerce_date_to_iso(tmp[period_key])
    tmp = tmp.dropna(subset=["_event"])
    tmp["_event"] = tmp["_event"].astype(str)

    tmp["_rowkey"] = tmp[row_key].astype(str)

    # Events (periods)
    events = sorted(set(tmp["_event"].tolist()))
    out["periods"] = events

    # Rows
    rows = sorted(set(tmp["_rowkey"].tolist()))
    out["rows"] = rows
    out["rowLabels"] = {rk: rk for rk in rows}

    # Variables
    for spec in variable_specs:
        if not isinstance(spec, Mapping):
            raise TypeError("variable_specs must be a list of dict-like specs")

        key = str(spec.get("key", "")).strip()
        candidates = spec.get("candidates") or ([key] if key else [])
        if not key:
            continue

        chosen = choose_first_present(tmp.columns, list(candidates))
        if not chosen:
            out["data"][key] = {}
            continue

        pv = (
            tmp.pivot_table(index="_rowkey", columns="_event", values=chosen, aggfunc="mean")
            .reindex(index=rows, columns=events)
        )

        block: Dict[str, Dict[str, Any]] = {}
        for rk in rows:
            rowvals: Dict[str, Any] = {}
            for ev in events:
                v = pv.at[rk, ev]
                rowvals[ev] = None if pd.isna(v) else float(v)
            block[rk] = rowvals

        out["data"][key] = block

    return out


def build_lab_table_payload_wide(
    df: pd.DataFrame,
    *,
    set_label: str,
    set_note: str,
    row_key: str,
    normalize_row_as_strip: bool = False,
    wide_variable_key: str = "value",
) -> Dict[str, Any]:
    """
    Wide dataset:
      first column = row_key
      remaining columns = event dates (often m/d/yy)
      single “variable” (e.g., biomass dry grams)
    """
    out = _payload_template(set_label, set_note)

    if row_key not in df.columns:
        raise ValueError(f"Missing row_key column for wide dataset: '{row_key}'")

    tmp = df.copy()

    event_cols = [c for c in tmp.columns if c != row_key]

    # Convert event headers to ISO if possible
    event_iso_map: Dict[str, str] = {}
    for c in event_cols:
        iso = pd.to_datetime(c, errors="coerce")
        event_iso_map[c] = str(c) if pd.isna(iso) else iso.strftime("%Y-%m-%d")

    # Preserve order, dedupe
    events = []
    seen: set[str] = set()
    for c in event_cols:
        e = event_iso_map[c]
        if e not in seen:
            events.append(e)
            seen.add(e)

    out["periods"] = events

    # Rows
    if normalize_row_as_strip:
        rows = tmp[row_key].map(normalize_strip).astype(str).tolist()
    else:
        rows = tmp[row_key].astype(str).tolist()

    out["rows"] = rows
    out["rowLabels"] = {rk: rk for rk in rows}

    # Single variable matrix
    matrix: Dict[str, Dict[str, Any]] = {}
    for _, r in tmp.iterrows():
        rk = normalize_strip(r[row_key]) if normalize_row_as_strip else str(r[row_key])
        rowvals: Dict[str, Any] = {}
        for c in event_cols:
            ev = event_iso_map[c]
            v = r[c]
            rowvals[ev] = None if pd.isna(v) else float(v)
        matrix[str(rk)] = rowvals

    out["data"][wide_variable_key] = matrix
    return out


# -----------------------------
# High-level entry point: build full table from config.table_specs
# -----------------------------

def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing lab source CSV: {path}")
    return pd.read_csv(path)


def build_lab_table(tab_key: str) -> Dict[str, Any]:
    """
    Generic builder for ALL lab-based wide tables.

    tab_key must match one of table_specs.WIDE_TABLES keys:
      "nir", "soilbio", "soilchem", "biomass_field"

    Returns:
      {
        "title": <tab title>,
        "tabKey": <tab_key>,
        "sourceKey": <source key>,
        "csv": <csv path>,
        "notes": <source notes>,
        "sets": [ setPayload, setPayload, ... ]
      }
    """
    table = table_specs.WIDE_TABLES.get(tab_key)
    if not table:
        raise ValueError(f"Unknown tab_key: {tab_key}")

    source = table_specs.TABLE_SOURCES.get(table.source_key)
    if not source:
        raise ValueError(f"Missing TableSourceSpec: {table.source_key}")

    df = _load_csv(source.csv_path)

    # If row_key was configured as "strip", normalize to STRIP 1..4
    normalize_row_as_strip = (source.row_key == "strip")

    sets_payload: List[Dict[str, Any]] = []

    for set_spec in table.sets:
        # set-specific note (optional) + source-level notes
        set_note = (set_spec.note or "").strip()

        if source.already_wide:
            # already-wide: one “value” variable, variables_key ignored
            sets_payload.append(
                build_lab_table_payload_wide(
                    df,
                    set_label=set_spec.label,
                    set_note=set_note,
                    row_key=source.row_key,
                    normalize_row_as_strip=normalize_row_as_strip,
                    wide_variable_key="value",
                )
            )
            continue

        if not source.period_key:
            raise ValueError(f"Source '{source.key}' requires period_key for pivoting")

        if not set_spec.variables_key:
            raise ValueError(
                f"Set '{set_spec.key}' for tab '{tab_key}' needs variables_key "
                f"(e.g., 'NIR_VARIABLES_SET1')"
            )

        variable_specs = getattr(table_specs, set_spec.variables_key, None)
        if not isinstance(variable_specs, list):
            raise ValueError(
                f"{set_spec.variables_key} must be defined in table_specs.py and be a list of dict specs"
            )

        sets_payload.append(
            build_lab_table_payload_long(
                df,
                set_label=set_spec.label,
                set_note=set_note,
                row_key=source.row_key,
                period_key=source.period_key,
                variable_specs=variable_specs,
                normalize_row_as_strip=normalize_row_as_strip,
            )
        )

    return {
        "title": table.label,
        "tabKey": table.key,
        "sourceKey": source.key,
        "csv": str(source.csv_path),
        "notes": source.notes,
        "sets": sets_payload,
    }