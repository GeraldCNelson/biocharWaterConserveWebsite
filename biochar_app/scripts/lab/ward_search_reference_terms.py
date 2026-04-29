#!/usr/bin/env python3
"""
ward_search_reference_terms.py

Search Ward source materials for reference terms across:
- .html
- .docx
- and standard text/code files

Also supports a schema coverage audit:
- flag variables that exist in your schema
- but do not appear in Ward source files at all

Typical uses
------------
1. General term search:
    python biochar_app/scripts/ward_search_reference_terms.py --root .

2. Search only Ward-like source files:
    python biochar_app/scripts/ward_search_reference_terms.py \
        --root . \
        --ext .html .htm .docx

3. Run schema audit only:
    python biochar_app/scripts/ward_search_reference_terms.py \
        --root . \
        --ext .html .htm .docx \
        --schema-audit

4. Run schema audit and include file name matches:
    python biochar_app/scripts/ward_search_reference_terms.py \
        --root . \
        --ext .html .htm .docx \
        --schema-audit \
        --show-filename-matches

Notes
-----
- .docx files are searched by reading word/document.xml inside the zip.
- Matching is done both with regex against original text and a normalized-text fallback.
- Schema audit is intentionally conservative: if nothing relevant is found in Ward
  source materials, the variable is flagged for review.
"""

from __future__ import annotations

import argparse
import re
import zipfile
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------

DEFAULT_TERMS = [
    "ivtdmd48_pctndf_db",
    "IVTDMD48",
    "IVTDMD",
    "In Vitro True Digestibility",
    "true digestibility",
    "digestibility",
    "48h",
    "% of NDF",
]

DEFAULT_EXTENSIONS = [
    ".py",
    ".html",
    ".htm",
    ".md",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
    ".csv",
    ".docx",
]

# These are the schema variables you most likely want to audit against Ward sources.
SCHEMA_KEYS_TO_CHECK = [
    "crude_protein_pct_db",
    "adf_pct_db",
    "ndf_pct_db",
    "tdn_pct_db",
    "rfv",
    "nfc_pct_db",
    "starch_pct_db",
    "wsc_pct_db",
    "fructan_pct_db",
    "nel_pct_db",
    "nem_pct_db",
    "neg_pct_db",
    "ash_pct_db",
    "ca_pct_db",
    "p_pct_db",
    "k_pct_db",
    "mg_pct_db",
    "ndfd48_pctndf_db",
    "ivtdmd48_pctndf_db",
    "fat_pct_db",
    "lignin_pct_db",
    "rfq",
]

# For each schema key, give a few search terms from most specific to more general.
# The first term is treated as the primary term for reporting.
SCHEMA_SEARCH_TERMS: Dict[str, List[str]] = {
    "crude_protein_pct_db": [
        "Crude Protein",
        "crude protein",
        "protein",
    ],
    "adf_pct_db": [
        "Acid Detergent Fiber",
        "ADF",
    ],
    "ndf_pct_db": [
        "Neutral Detergent Fiber",
        "NDF",
    ],
    "tdn_pct_db": [
        "Total Digestible Nutrients",
        "TDN",
    ],
    "rfv": [
        "Relative Feed Value",
        "RFV",
    ],
    "nfc_pct_db": [
        "Non-Fiber Carbohydrates",
        "Non Fiber Carbohydrates",
        "NFC",
    ],
    "starch_pct_db": [
        "Starch",
        "Total Starch",
    ],
    "wsc_pct_db": [
        "Water-Soluble Carbohydrates",
        "Water Soluble Carbohydrates",
        "WSC",
    ],
    "fructan_pct_db": [
        "Fructan",
        "Fructans",
    ],
    "nel_pct_db": [
        "Net Energy for Lactation",
        "NEL",
        "NEl",
        "Net Energy",
    ],
    "nem_pct_db": [
        "Net Energy for Maintenance",
        "NEM",
        "NEm",
        "Net Energy",
    ],
    "neg_pct_db": [
        "Net Energy for Gain",
        "NEG",
        "NEg",
        "Net Energy",
    ],
    "ash_pct_db": [
        "Ash",
    ],
    "ca_pct_db": [
        "Calcium",
        "Ca",
    ],
    "p_pct_db": [
        "Phosphorus",
        "P",
    ],
    "k_pct_db": [
        "Potassium",
        "K",
    ],
    "mg_pct_db": [
        "Magnesium",
        "Mg",
    ],
    "ndfd48_pctndf_db": [
        "NDFD48",
        "NDFD 48",
        "NDF Digestibility",
        "NDF digestibility",
        "48h",
    ],
    "ivtdmd48_pctndf_db": [
        "IVTDMD48",
        "IVTDMD",
        "IVTDMD 48 hr",
        "IVTDMD 48h",
        "In Vitro True Digestibility",
        "in vitro true digestibility",
        "true digestibility",
    ],
    "fat_pct_db": [
        "Fat",
        "Crude Fat",
    ],
    "lignin_pct_db": [
        "Lignin",
    ],
    "rfq": [
        "Relative Forage Quality",
        "RFQ",
    ],
}


# ---------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------

@dataclass
class MatchRecord:
    file_path: Path
    line_no: int
    term: str
    line_text: str
    context_before: List[str]
    context_after: List[str]


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="Root directory to scan")
    parser.add_argument("--term", action="append", default=[], help="Search term (repeatable)")
    parser.add_argument("--ext", nargs="*", default=DEFAULT_EXTENSIONS, help="Extensions to include")
    parser.add_argument("--case-sensitive", action="store_true")
    parser.add_argument("--whole-word", action="store_true")
    parser.add_argument("--context", type=int, default=1)
    parser.add_argument("--max-matches-per-file", type=int, default=20)
    parser.add_argument("--show-filename-matches", action="store_true")
    parser.add_argument("--include-hidden", action="store_true")
    parser.add_argument(
        "--schema-audit",
        action="store_true",
        help="Audit schema keys against Ward source materials",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[_\-/]+", " ", text)
    text = re.sub(r"[^a-z0-9% ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_pattern(term: str, case_sensitive: bool, whole_word: bool) -> re.Pattern[str]:
    escaped = re.escape(term)
    pattern = rf"\b{escaped}\b" if whole_word else escaped
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(pattern, flags)


def should_skip(path: Path, include_hidden: bool) -> bool:
    if include_hidden:
        return False
    return any(part.startswith(".") for part in path.parts)


def iter_files(root: Path, extensions: Sequence[str], include_hidden: bool) -> Iterable[Path]:
    extset = {e.lower() for e in extensions}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if should_skip(rel, include_hidden):
            continue
        if p.suffix.lower() in extset:
            yield p


# ---------------------------------------------------------------------
# DOCX handling
# ---------------------------------------------------------------------

def extract_docx_text(path: Path) -> str | None:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            xml = zf.read("word/document.xml")
    except Exception:
        return None

    text = xml.decode("utf-8", errors="replace")
    text = re.sub(r"</w:p>", "\n", text)
    text = re.sub(r"<w:br\s*/?>", "\n", text)
    text = re.sub(r"<w:tab\s*/?>", "\t", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def read_file(path: Path) -> str | None:
    if path.suffix.lower() == ".docx":
        return extract_docx_text(path)

    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


# ---------------------------------------------------------------------
# Search logic
# ---------------------------------------------------------------------

def search_file(
    path: Path,
    terms: Sequence[str],
    case_sensitive: bool,
    whole_word: bool,
    context: int,
    max_matches: int,
) -> List[MatchRecord]:
    text = read_file(path)
    if not text:
        return []

    lines = text.splitlines()
    normalized_lines = [normalize_text(line) for line in lines]
    normalized_terms = {term: normalize_text(term) for term in terms}
    patterns = {term: build_pattern(term, case_sensitive, whole_word) for term in terms}

    results: List[MatchRecord] = []

    for i, line in enumerate(lines):
        for term, pattern in patterns.items():
            match_found = bool(pattern.search(line))

            if not match_found:
                if normalized_terms[term] and normalized_terms[term] in normalized_lines[i]:
                    match_found = True

            if not match_found:
                continue

            before = lines[max(0, i - context):i]
            after = lines[i + 1:i + 1 + context]

            results.append(
                MatchRecord(
                    file_path=path,
                    line_no=i + 1,
                    term=term,
                    line_text=line,
                    context_before=before,
                    context_after=after,
                )
            )

            if len(results) >= max_matches:
                return results

    return results


def filename_matches(path: Path, terms: Sequence[str], case_sensitive: bool) -> List[str]:
    name = str(path)
    haystack = name if case_sensitive else name.lower()

    hits: List[str] = []
    for term in terms:
        needle = term if case_sensitive else term.lower()
        if needle in haystack:
            hits.append(term)
    return hits


def print_match(m: MatchRecord) -> None:
    print(f"\n{m.file_path}:{m.line_no}  [term: {m.term}]")
    for line in m.context_before:
        print(f"    {line}")
    print(f"--> {m.line_text}")
    for line in m.context_after:
        print(f"    {line}")


# ---------------------------------------------------------------------
# Schema audit
# ---------------------------------------------------------------------

def search_schema_key_in_sources(
    schema_key: str,
    source_files: Sequence[Path],
    case_sensitive: bool,
    whole_word: bool,
    context: int,
    max_matches_per_file: int,
) -> Dict[str, List[MatchRecord]]:
    """
    Returns dict term -> matches for that schema key.
    """
    terms = SCHEMA_SEARCH_TERMS.get(schema_key, [schema_key])
    out: Dict[str, List[MatchRecord]] = {term: [] for term in terms}

    for path in source_files:
        file_matches = search_file(
            path=path,
            terms=terms,
            case_sensitive=case_sensitive,
            whole_word=whole_word,
            context=context,
            max_matches=max_matches_per_file,
        )
        for m in file_matches:
            out[m.term].append(m)

    return out


def summarize_schema_audit(
    source_files: Sequence[Path],
    case_sensitive: bool,
    whole_word: bool,
    context: int,
    max_matches_per_file: int,
) -> int:
    print("\n--- SCHEMA COVERAGE AUDIT ---")
    print(f"Schema keys checked: {len(SCHEMA_KEYS_TO_CHECK)}")
    print(f"Source files used : {len(source_files)}")

    found_primary: List[str] = []
    found_fallback_only: List[str] = []
    missing_all: List[str] = []

    for schema_key in SCHEMA_KEYS_TO_CHECK:
        term_hits = search_schema_key_in_sources(
            schema_key=schema_key,
            source_files=source_files,
            case_sensitive=case_sensitive,
            whole_word=whole_word,
            context=context,
            max_matches_per_file=max_matches_per_file,
        )

        search_terms = SCHEMA_SEARCH_TERMS.get(schema_key, [schema_key])
        primary_term = search_terms[0]

        primary_hits = term_hits.get(primary_term, [])
        total_hits = sum(len(v) for v in term_hits.values())

        if primary_hits:
            found_primary.append(schema_key)
            print(f"✅ {schema_key}: found primary term '{primary_term}' ({len(primary_hits)} hit(s))")
            first = primary_hits[0]
            print(f"   first hit: {first.file_path}:{first.line_no}")
            print(f"   text     : {first.line_text.strip()}")
            continue

        if total_hits > 0:
            found_fallback_only.append(schema_key)
            terms_with_hits = [t for t, hits in term_hits.items() if hits]
            print(f"⚠️ {schema_key}: no primary-term hit, but found fallback term(s): {terms_with_hits}")
            first_term = terms_with_hits[0]
            first = term_hits[first_term][0]
            print(f"   first hit: {first.file_path}:{first.line_no}")
            print(f"   text     : {first.line_text.strip()}")
            continue

        missing_all.append(schema_key)
        print(f"❌ {schema_key}: no evidence found in Ward source files")

    print("\n--- SCHEMA AUDIT SUMMARY ---")
    print(f"Primary-term supported : {len(found_primary)}")
    print(f"Fallback-only supported: {len(found_fallback_only)}")
    print(f"No source support found: {len(missing_all)}")

    if found_fallback_only:
        print("\nFallback-only variables:")
        for key in found_fallback_only:
            print(f"  - {key}")

    if missing_all:
        print("\nVariables with no Ward-source support found:")
        for key in missing_all:
            print(f"  - {key}")

    return len(missing_all)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    root = Path(args.root).resolve()
    terms = args.term if args.term else DEFAULT_TERMS

    print(f"Root: {root}")
    print(f"Terms: {terms}")
    print(f"Extensions: {args.ext}")

    files = list(iter_files(root, args.ext, args.include_hidden))
    print(f"✅ Scanning {len(files)} files")

    total_matches = 0
    files_with_matches = 0

    if args.show_filename_matches:
        print("\n--- FILENAME MATCHES ---")
        for f in files:
            hits = filename_matches(f, terms, args.case_sensitive)
            if hits:
                print(f"{f} <- {hits}")

    print("\n--- CONTENT MATCHES ---")

    for f in files:
        matches = search_file(
            path=f,
            terms=terms,
            case_sensitive=args.case_sensitive,
            whole_word=args.whole_word,
            context=args.context,
            max_matches=args.max_matches_per_file,
        )

        if not matches:
            continue

        files_with_matches += 1
        total_matches += len(matches)

        print(f"\n### {f} ({len(matches)} match(es)) ###")
        for m in matches:
            print_match(m)

    print("\n--- SUMMARY ---")
    print(f"Files scanned      : {len(files)}")
    print(f"Files with matches : {files_with_matches}")
    print(f"Total matches      : {total_matches}")

    missing_count = 0
    if args.schema_audit:
        missing_count = summarize_schema_audit(
            source_files=files,
            case_sensitive=args.case_sensitive,
            whole_word=args.whole_word,
            context=args.context,
            max_matches_per_file=args.max_matches_per_file,
        )

    print("✅ Search complete.")

    # Return nonzero only if schema audit was requested and uncovered unsupported vars.
    if args.schema_audit and missing_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())