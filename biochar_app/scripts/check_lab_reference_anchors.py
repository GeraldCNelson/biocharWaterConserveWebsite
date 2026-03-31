#!/usr/bin/env python3
"""
check_lab_reference_anchors.py

Validate that every source_url fragment used in LAB_REFERENCES points to a real
HTML id on the rendered reference pages served by the app.

What it checks
--------------
1. Imports LAB_REFERENCES from biochar_app.config.lab_reference_data
2. Walks every VariableReferenceBundle and every ReferenceInfo inside it
3. Looks at source_url values like:
       /lab-references/ward-guide#table-18
4. Downloads the rendered HTML page from a local base URL
5. Extracts all ids from the HTML
6. Verifies that the fragment exists exactly
7. If not, suggests likely matching ids
8. Runs semantic checks for anchors that technically exist but may still be
   poor landing targets for the variable

Default base URL
----------------
    http://127.0.0.1:8000

Examples
--------
Run against local dev server:
    python biochar_app/scripts/check_lab_reference_anchors.py

Specify a different host:
    python biochar_app/scripts/check_lab_reference_anchors.py --base-url http://127.0.0.1:5000

Only check one guide page:
    python biochar_app/scripts/check_lab_reference_anchors.py --only /lab-references/ward-guide

Treat semantic warnings as failures:
    python biochar_app/scripts/check_lab_reference_anchors.py --strict-semantic

Notes
-----
- This validates against the rendered HTML actually served by your app.
- Fragments are only checked when source_url contains a '#'.
- URLs without a fragment are reported as skipped, not failed.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import urlopen


def ok(msg: str) -> None:
    print(f"✅ {msg}")


def warn(msg: str) -> None:
    print(f"⚠️ {msg}")


def fail(msg: str) -> None:
    print(f"❌ {msg}")


class IdCollector(HTMLParser):
    """Collect all HTML id attributes from a page."""

    def __init__(self) -> None:
        super().__init__()
        self.ids: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        for key, value in attrs:
            if key == "id" and value:
                self.ids.append(value)


@dataclass
class AnchorRecord:
    lab_key: str
    ref_index: int
    guide_label: Optional[str]
    section_title: Optional[str]
    table_number: Optional[str]
    table_title: Optional[str]
    source_url: str
    page_path: str
    fragment: str


@dataclass
class SemanticIssue:
    level: str  # "warn" or "fail"
    message: str


def normalize_table_number(text: Optional[str]) -> Optional[str]:
    """
    Convert 'Table 18' -> 'table-18'
    Convert 'Table 7'  -> 'table-7'
    """
    if not text:
        return None
    m = re.search(r"table\s+(\d+)", text.strip(), flags=re.IGNORECASE)
    if not m:
        return None
    return f"table-{m.group(1)}"


def fetch_html(url: str, timeout: int = 15) -> str:
    with urlopen(url, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def extract_ids(html: str) -> List[str]:
    parser = IdCollector()
    parser.feed(html)
    return parser.ids


def simple_slug(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def score_candidate(fragment: str, candidate: str, table_hint: Optional[str], title_hint: Optional[str]) -> int:
    """
    Rough ranking for suggested ids.
    Higher is better.
    """
    score = 0
    f = fragment.lower()
    c = candidate.lower()

    if c == f:
        score += 1000
    if c.startswith(f):
        score += 300
    if f in c:
        score += 150

    if table_hint:
        th = table_hint.lower()
        if c.startswith(th):
            score += 200
        if th in c:
            score += 100

    if title_hint:
        title_slug = simple_slug(title_hint)
        if title_slug and title_slug in c:
            score += 250

    score -= max(len(c) - len(f), 0)
    return score


def suggest_matches(
    fragment: str,
    ids_on_page: Sequence[str],
    table_number: Optional[str],
    table_title: Optional[str],
    limit: int = 5,
) -> List[str]:
    table_hint = normalize_table_number(table_number)
    ranked: List[Tuple[int, str]] = []

    for candidate in ids_on_page:
        score = score_candidate(fragment, candidate, table_hint, table_title)
        if score > 0:
            ranked.append((score, candidate))

    ranked.sort(key=lambda x: (-x[0], x[1]))
    out: List[str] = []
    seen = set()
    for _score, cand in ranked:
        if cand not in seen:
            seen.add(cand)
            out.append(cand)
        if len(out) >= limit:
            break
    return out


def build_anchor_records(
    lab_references: Dict[str, object],
    only_page: Optional[str] = None,
) -> Tuple[List[AnchorRecord], List[Tuple[str, int, str]]]:
    """
    Returns:
      - records with fragments to validate
      - skipped refs without fragments: (lab_key, ref_index, source_url)
    """
    records: List[AnchorRecord] = []
    skipped: List[Tuple[str, int, str]] = []

    for lab_key, bundle in lab_references.items():
        if bundle is None:
            continue

        refs = getattr(bundle, "references", ()) or ()
        for idx, ref in enumerate(refs, start=1):
            source_url = getattr(ref, "source_url", None)
            if not source_url:
                continue

            parts = urlsplit(source_url)
            page_path = parts.path
            fragment = parts.fragment

            if only_page and page_path != only_page:
                continue

            if not fragment:
                skipped.append((lab_key, idx, source_url))
                continue

            records.append(
                AnchorRecord(
                    lab_key=lab_key,
                    ref_index=idx,
                    guide_label=getattr(ref, "guide_label", None),
                    section_title=getattr(ref, "section_title", None),
                    table_number=getattr(ref, "table_number", None),
                    table_title=getattr(ref, "table_title", None),
                    source_url=source_url,
                    page_path=page_path,
                    fragment=fragment,
                )
            )

    return records, skipped


def semantic_checks(rec: AnchorRecord) -> List[SemanticIssue]:
    """
    Checks for anchors that exist but may still be poor semantic matches.

    Current rules:
    - Derived NDF digestibility variables should usually reference the NDF section
      somewhere in their reference bundle, not only the generic NIRS section.
    - Net energy variables should usually reference the Energy Values / Net Energy
      section somewhere in their bundle, not only the generic NIRS section.
    - Generic NIRS-only anchors on derived variables are treated as suspicious.
    """
    issues: List[SemanticIssue] = []
    key = rec.lab_key
    frag = rec.fragment.lower()
    section = (rec.section_title or "").lower()
    title = (rec.table_title or "").lower()

    # NDF digestibility / IVTDMD derived fiber-digestibility metrics
    if key in {"ndfd48_pctndf_db", "ivtdmd48_pctndf_db"}:
        good_parent_targets = {
            "neutral-detergent-fiber-ndf",
            "acid-detergent-fiber-adf",
            "relative-forage-quality-rfq",
        }
        if frag == "near-infrared-spectroscopy-nirs":
            issues.append(
                SemanticIssue(
                    level="warn",
                    message=(
                        f"{key} uses only the generic NIRS anchor. "
                        "Consider adding a parent-concept anchor such as "
                        "#neutral-detergent-fiber-ndf, #acid-detergent-fiber-adf, or #relative-forage-quality-rfq."
                    ),
                )
            )
        elif frag not in good_parent_targets:
            issues.append(
                SemanticIssue(
                    level="warn",
                    message=(
                        f"{key} resolves to #{rec.fragment}, which exists, but this may not be the best parent concept. "
                        "Consider whether #neutral-detergent-fiber-ndf would be more informative."
                    ),
                )
            )

    # Net energy derived variables
    if key in {"nel_pct_db", "nem_pct_db", "neg_pct_db"}:
        energy_targets = {
            "energy-values",
            "energy-values-1",
            "net-energy-ne",
            "total-digestible-nutrients-tdn",
        }
        if frag == "near-infrared-spectroscopy-nirs":
            issues.append(
                SemanticIssue(
                    level="warn",
                    message=(
                        f"{key} uses only the generic NIRS anchor. "
                        "Consider adding an energy-related anchor such as "
                        "#net-energy-ne, #energy-values, #energy-values-1, or #total-digestible-nutrients-tdn."
                    ),
                )
            )
        elif frag not in energy_targets:
            issues.append(
                SemanticIssue(
                    level="warn",
                    message=(
                        f"{key} resolves to #{rec.fragment}, which exists, but this may not be the clearest energy landing section."
                    ),
                )
            )

    # RFV/RFQ should usually not point only to generic NIRS
    if key in {"rfv", "rfq"} and frag == "near-infrared-spectroscopy-nirs":
        issues.append(
            SemanticIssue(
                level="warn",
                message=(
                    f"{key} uses the generic NIRS anchor. A more specific parent section or table is usually better "
                    "(for example RFV/RFQ section or Table 7)."
                ),
            )
        )

    # Starch/WSC/Fructan/NFC often benefit from something beyond generic NIRS
    if key in {"nfc_pct_db", "starch_pct_db", "wsc_pct_db", "fructan_pct_db"} and frag == "near-infrared-spectroscopy-nirs":
        issues.append(
            SemanticIssue(
                level="warn",
                message=(
                    f"{key} uses only the generic NIRS anchor. This is valid, but you may want a more concept-specific landing target if one exists."
                ),
            )
        )

    # Table-number/title mismatch sanity check
    if rec.table_number and rec.table_title:
        table_hint = normalize_table_number(rec.table_number)
        if table_hint and not frag.startswith(table_hint):
            issues.append(
                SemanticIssue(
                    level="warn",
                    message=(
                        f"{key} uses {rec.table_number} with fragment #{rec.fragment}, which does not begin with {table_hint}. "
                        "This may still be intentional, but it is worth verifying."
                    ),
                )
            )

    # Section title anchor mismatch sanity check
    if rec.section_title:
        expected_slug = simple_slug(rec.section_title)
        if expected_slug and expected_slug != frag:
            # Do not warn when table_number is being used instead of section title
            if not rec.table_number:
                # Be lenient for cases where the current anchor is still a prefix/similar form
                if expected_slug not in frag and frag not in expected_slug:
                    issues.append(
                        SemanticIssue(
                            level="warn",
                            message=(
                                f"{key} section title '{rec.section_title}' would normally slug to #{expected_slug}, "
                                f"but source_url uses #{rec.fragment}. Verify that this is intentional."
                            ),
                        )
                    )

    return issues


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL for the locally served app. Default: http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--only",
        default=None,
        help="Optional page path to check only one guide page, e.g. /lab-references/ward-guide",
    )
    parser.add_argument(
        "--show-skipped",
        action="store_true",
        help="Also print source_url entries that have no fragment.",
    )
    parser.add_argument(
        "--strict-semantic",
        action="store_true",
        help="Treat semantic warnings as failures.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from biochar_app.config.lab_reference_data import LAB_REFERENCES
    except Exception as e:
        fail(f"Import failed: {e}")
        return 1

    ok(f"Imported LAB_REFERENCES successfully ({len(LAB_REFERENCES)} keys)")

    records, skipped = build_anchor_records(LAB_REFERENCES, only_page=args.only)

    if not records and not skipped:
        warn("No reference URLs found to check.")
        return 0

    if skipped:
        warn(f"{len(skipped)} reference URL(s) have no fragment and were skipped.")
        if args.show_skipped:
            for lab_key, idx, source_url in skipped:
                print(f"   - {lab_key} [ref {idx}] -> {source_url}")

    by_page: Dict[str, List[AnchorRecord]] = defaultdict(list)
    for rec in records:
        by_page[rec.page_path].append(rec)

    page_ids: Dict[str, List[str]] = {}
    page_errors: Dict[str, str] = {}

    print("\n--- FETCHING PAGES ---")
    for page_path in sorted(by_page):
        full_url = urljoin(args.base_url.rstrip("/") + "/", page_path.lstrip("/"))
        try:
            html = fetch_html(full_url)
            ids = extract_ids(html)
            page_ids[page_path] = ids
            ok(f"Fetched {page_path} ({len(ids)} ids)")
        except HTTPError as e:
            msg = f"HTTP {e.code} for {full_url}"
            page_errors[page_path] = msg
            fail(msg)
        except URLError as e:
            msg = f"URL error for {full_url}: {e}"
            page_errors[page_path] = msg
            fail(msg)
        except Exception as e:
            msg = f"Failed to fetch {full_url}: {e}"
            page_errors[page_path] = msg
            fail(msg)

    print("\n--- ANCHOR CHECK ---")
    total_checked = 0
    total_ok = 0
    total_bad = 0
    total_semantic_warn = 0
    total_semantic_fail = 0

    for page_path in sorted(by_page):
        print(f"\n[{page_path}]")
        if page_path in page_errors:
            for rec in by_page[page_path]:
                total_checked += 1
                total_bad += 1
                fail(
                    f"{rec.lab_key} [ref {rec.ref_index}] could not be checked "
                    f"because page fetch failed: {page_errors[page_path]}"
                )
            continue

        ids_on_page = page_ids.get(page_path, [])
        ids_set = set(ids_on_page)

        for rec in by_page[page_path]:
            total_checked += 1
            label = rec.table_number or rec.section_title or rec.table_title or f"ref {rec.ref_index}"

            if rec.fragment in ids_set:
                total_ok += 1
                ok(f"{rec.lab_key} / {label} -> #{rec.fragment}")

                semantic_issues = semantic_checks(rec)
                for issue in semantic_issues:
                    if issue.level == "fail" or args.strict_semantic:
                        total_semantic_fail += 1
                        fail(f"{rec.lab_key} semantic check: {issue.message}")
                    else:
                        total_semantic_warn += 1
                        warn(f"{rec.lab_key} semantic check: {issue.message}")
                continue

            total_bad += 1
            fail(f"{rec.lab_key} / {label} -> #{rec.fragment} not found")

            suggestions = suggest_matches(
                fragment=rec.fragment,
                ids_on_page=ids_on_page,
                table_number=rec.table_number,
                table_title=rec.table_title,
            )

            print(f"   source_url: {rec.source_url}")
            if rec.table_title:
                print(f"   table_title: {rec.table_title}")
            if rec.section_title:
                print(f"   section_title: {rec.section_title}")

            if suggestions:
                print("   possible matches:")
                for s in suggestions:
                    print(f"     - {s}")
            else:
                table_hint = normalize_table_number(rec.table_number)
                if table_hint:
                    prefix_matches = [x for x in ids_on_page if x.startswith(table_hint)]
                    if prefix_matches:
                        print("   ids starting with table number:")
                        for s in prefix_matches[:5]:
                            print(f"     - {s}")
                    else:
                        print("   no obvious match found")
                else:
                    print("   no obvious match found")

    print("\n--- SUMMARY ---")
    print(f"Checked: {total_checked}")
    print(f"Passed : {total_ok}")
    print(f"Failed : {total_bad}")
    print(f"Skipped: {len(skipped)}")
    print(f"Semantic warnings: {total_semantic_warn}")
    print(f"Semantic failures: {total_semantic_fail}")

    hard_failures = total_bad + total_semantic_fail
    if hard_failures == 0:
        ok("All checked anchors resolved to real ids.")
        if total_semantic_warn:
            warn("All anchors exist, but some semantic anchor-quality warnings were found.")
        return 0

    fail("Some anchors did not match rendered HTML ids or failed semantic checks.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())