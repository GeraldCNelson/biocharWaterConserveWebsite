#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from biochar_app.config.paths import (
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    WARD_MASTER_NIR_CSV,
)

GLOSSARY_JSON = Path("biochar_app/static/data/glossary_terms.json")
OUT_CSV = Path("biochar_app/data-processed/glossary_alias_suggestions.csv")


DATASETS = {
    "soil_biology": WARD_MASTER_SOILBIO_CSV,
    "soil_chemistry": WARD_MASTER_SOILCHEM_CSV,
    "plant_forage_metrics": WARD_MASTER_NIR_CSV,
}


SCIENTIFIC_ALIASES = {
    "monounsaturated_polyunsaturated_ratio": [
        "monounsaturated polyunsaturated ratio",
        "monounsaturated:polyunsaturated ratio",
        "mono poly ratio",
        "MUFA PUFA ratio",
        "MUFA:PUFA ratio",
        "microbial stress indicator",
    ],
    "saturated_unsaturated_ratio": [
        "saturated unsaturated ratio",
        "saturated:unsaturated ratio",
        "saturated fatty acid unsaturated fatty acid ratio",
        "microbial stress ratio",
        "PLFA stress indicator",
    ],
    "gram_pos_gram_neg_ratio": [
        "gram positive gram negative ratio",
        "gram-positive gram-negative ratio",
        "Gram(+):Gram(-) ratio",
        "Gram positive to Gram negative ratio",
    ],
    "fungi_bacteria": [
        "fungal bacterial ratio",
        "fungal:bacterial ratio",
        "fungi bacteria ratio",
        "fungi:bacteria ratio",
    ],
    "predator_prey": [
        "predator prey ratio",
        "predator:prey ratio",
        "soil food web ratio",
    ],
    "ndfd48_pctndf_db": [
        "NDF digestibility",
        "NDFD 48",
        "NDFD48",
        "neutral detergent fiber digestibility",
        "48 hour NDF digestibility",
    ],
    "ivtdmd48_pctndf_db": [
        "in vitro true digestibility",
        "IVTDMD",
        "IVTDMD 48",
        "48 hour digestibility",
    ],
    "rfv": [
        "relative feed value",
        "forage quality index",
    ],
    "rfq": [
        "relative forage quality",
        "forage quality index",
    ],
}


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def humanize_column(col: str) -> str:
    s = str(col)

    replacements = {
        "_pct_db": " percent dry basis",
        "_pctndf_db": " percent ndf dry basis",
        "_pct": " percent",
        "_ppm": " ppm",
        "_meq_100g": " meq per 100g",
        "_me_100g": " meq per 100g",
        "_lb_per_acre": " pounds per acre",
        "_total_lb": " total pounds",
        "_biomass": " biomass",
        "_ratio": " ratio",
        "_rec": " recommendation",
        "_": " ",
    }

    for old, new in replacements.items():
        s = s.replace(old, new)

    return normalize_text(s)


def load_glossary() -> dict:
    return json.loads(GLOSSARY_JSON.read_text(encoding="utf-8"))


def existing_aliases(item: dict) -> set[str]:
    vals = []
    vals.append(item.get("term", ""))
    vals.append(item.get("abbreviation", ""))
    vals.extend(item.get("matches", []) or [])
    vals.extend(item.get("aliases", []) or [])
    return {normalize_text(v) for v in vals if str(v).strip()}


def item_matches_column(item: dict, col: str) -> bool:
    col_norm = normalize_text(col)
    col_tokens = set(col_norm.split())

    candidates = []
    candidates.append(item.get("key", ""))
    candidates.append(item.get("term", ""))
    candidates.extend(item.get("matches", []) or [])

    # Do NOT use abbreviation alone for matching columns.
    # Single-letter abbreviations like N, P, K, S cause massive false matches.

    for cand in candidates:
        cand_norm = normalize_text(cand)
        if not cand_norm:
            continue

        cand_tokens = set(cand_norm.split())

        # Exact normalized phrase match
        if cand_norm == col_norm:
            return True

        # Multi-token phrase contained in column
        if len(cand_tokens) >= 2 and cand_tokens.issubset(col_tokens):
            return True

        # Machine-name style exact/near-exact match
        cand_machine = cand_norm.replace(" ", "_")
        col_machine = col_norm.replace(" ", "_")

        if len(cand_machine) > 2:
            if col_machine == cand_machine:
                return True
            if col_machine.startswith(f"{cand_machine}_"):
                return True
            if f"_{cand_machine}_" in f"_{col_machine}_":
                return True

    return False


def suggest_for_item(section_key: str, item: dict, columns: list[str]) -> list[dict]:
    current = existing_aliases(item)
    suggestions: set[str] = set()

    key = str(item.get("key", ""))

    for col in columns:
        if item_matches_column(item, col):
            suggestions.add(col)
            suggestions.add(humanize_column(col))

    for match in item.get("matches", []) or []:
        suggestions.add(str(match))
        suggestions.add(humanize_column(str(match)))

    for alias in SCIENTIFIC_ALIASES.get(key, []):
        suggestions.add(alias)

    clean_suggestions = []
    for s in sorted(suggestions, key=str.lower):
        ns = normalize_text(s)
        if not ns or ns in current:
            continue
        clean_suggestions.append(s)

    return [
        {
            "section_key": section_key,
            "glossary_key": item.get("key", ""),
            "term": item.get("term", ""),
            "suggested_alias": s,
        }
        for s in clean_suggestions
    ]


def main() -> None:
    glossary = load_glossary()
    rows = []

    dataset_columns = {}
    for section_key, csv_path in DATASETS.items():
        if Path(csv_path).exists():
            dataset_columns[section_key] = pd.read_csv(csv_path, nrows=1).columns.astype(str).tolist()
        else:
            dataset_columns[section_key] = []

    for section in glossary.get("sections", []):
        section_key = section.get("key", "")
        if section_key not in DATASETS:
            continue

        columns = dataset_columns.get(section_key, [])

        for item in section.get("items", []):
            rows.extend(suggest_for_item(section_key, item, columns))

    out = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Wrote {len(out)} alias suggestions:")
    print(OUT_CSV)


if __name__ == "__main__":
    main()