#!/usr/bin/env python3
"""
Sanity checks for lab_reference_data.py

Run:
    python biochar_app/scripts/check_lab_reference_data.py
"""

from __future__ import annotations


def fail(msg: str) -> None:
    print(f"❌ {msg}")


def ok(msg: str) -> None:
    print(f"✅ {msg}")


def warn(msg: str) -> None:
    print(f"⚠️ {msg}")


def ref_titles(bundle) -> list[str]:
    out = []
    if bundle is None:
        return out

    for ref in bundle.references:
        title = ref.section_title or ref.table_title or "(untitled)"
        out.append(title)
    return out


def contains(bundle, text: str) -> bool:
    text = text.lower()
    return any(text in t.lower() for t in ref_titles(bundle))


def check_keys_exist(lab_references: dict, keys: list[str], label: str) -> None:
    print(f"\n--- {label} KEY EXISTENCE CHECK ---")
    missing = [k for k in keys if k not in lab_references]
    if missing:
        fail(f"Missing keys: {missing}")
    else:
        ok(f"All {label.lower()} keys present")


def check_bundle_presence(lab_references: dict, keys: list[str], label: str) -> None:
    print(f"\n--- {label} BUNDLE CHECK ---")

    for key in keys:
        bundle = lab_references.get(key)

        if bundle is None:
            fail(f"{key}: missing or None")
            continue

        titles = ref_titles(bundle)

        print(f"\n{key}:")
        for t in titles:
            print(f"  - {t}")

        if titles:
            ok(f"{key}: has reference bundle")
        else:
            fail(f"{key}: bundle has no references")


def main() -> int:
    try:
        from biochar_app.config.lab_reference_data import LAB_REFERENCES
    except Exception as e:
        fail(f"Import failed: {e}")
        return 1

    ok("Imported successfully")
    ok(f"LAB_REFERENCES size: {len(LAB_REFERENCES)}")

    # ------------------------------------------------------------------
    # REQUIRED NIR KEYS
    # ------------------------------------------------------------------
    nir_primary = [
        "crude_protein_pct_db",
        "adf_pct_db",
        "ndf_pct_db",
        "tdn_pct_db",
        "rfv",
        "rfq",
    ]

    nir_fallback = [
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
    ]

    # ------------------------------------------------------------------
    # SOIL BIO / PLFA KEYS
    # ------------------------------------------------------------------
    soil_bio_keys = [
        "total_biomass",
        "bacteria_biomass",
        "fungi_biomass",
        "diversity_index",
        "fungi_bacteria",
        "predator_prey",
        "gram_pos_gram",
        "mycorrhizae_biomass",
        "actinobacteria_biomass",
        "rhizobia_biomass",
        "saprophytes_biomass",
        "undifferentiated_biomass",
        "pre_16_1w7c_cy17_0",
        "pre_18_1w7c_cy19_0",
        "sat_unsat",
        "mono_poly",
    ]

    # ------------------------------------------------------------------
    # SOIL CHEM / SOIL HEALTH SPOT CHECK KEYS
    # ------------------------------------------------------------------
    soil_chem_keys = [
        "ph",
        "buffer_ph",
        "salinity",
        "excess_lime",
        "phosphorus",
        "potassium",
        "sulfur",
        "nitrate",
        "calcium",
        "magnesium",
        "sodium",
        "zinc",
        "iron",
        "manganese",
        "copper",
        "cec",
        "base_saturation",
        "water_stable_aggregates",
        "soil_respiration",
        "weoc",
        "weon",
        "organic_cn",
        "soil_health_score",
        "mac",
    ]

    # ------------------------------------------------------------------
    # KEY EXISTENCE CHECKS
    # ------------------------------------------------------------------
    check_keys_exist(LAB_REFERENCES, nir_primary + nir_fallback, "NIR")
    check_keys_exist(LAB_REFERENCES, soil_bio_keys, "SOIL BIO / PLFA")
    check_keys_exist(LAB_REFERENCES, soil_chem_keys, "SOIL CHEM / HEALTH")

    # ------------------------------------------------------------------
    # PRIMARY NIR VARIABLES: MUST HAVE SPECIFIC REFERENCE + NIRS
    # ------------------------------------------------------------------
    print("\n--- PRIMARY NIR REFERENCE CHECK ---")

    expected_titles = {
        "crude_protein_pct_db": "Protein",
        "adf_pct_db": "Acid Detergent Fiber",
        "ndf_pct_db": "Neutral Detergent Fiber",
        "tdn_pct_db": "Total Digestible Nutrients",
        "rfv": "Relative Feed Value",
        "rfq": "Relative Forage Quality",
    }

    for key, expected in expected_titles.items():
        bundle = LAB_REFERENCES.get(key)

        if bundle is None:
            fail(f"{key}: missing entirely")
            continue

        titles = ref_titles(bundle)

        print(f"\n{key}:")
        for t in titles:
            print(f"  - {t}")

        has_specific = contains(bundle, expected)
        has_nirs = contains(bundle, "Near-Infrared Spectroscopy")

        if has_specific:
            ok(f"{key}: has specific reference")
        else:
            fail(f"{key}: missing specific reference ({expected})")

        if has_nirs:
            ok(f"{key}: has NIRS reference")
        else:
            fail(f"{key}: missing NIRS reference")

    # ------------------------------------------------------------------
    # FALLBACK NIR VARIABLES: MUST INCLUDE NIRS
    # ------------------------------------------------------------------
    print("\n--- FALLBACK NIR CHECK ---")

    for key in nir_fallback:
        bundle = LAB_REFERENCES.get(key)

        if bundle is None:
            fail(f"{key}: missing")
            continue

        if contains(bundle, "Near-Infrared Spectroscopy"):
            ok(f"{key}: OK")
        else:
            fail(f"{key}: missing NIRS reference")

    # ------------------------------------------------------------------
    # SOIL BIO / PLFA BUNDLE CHECK
    # ------------------------------------------------------------------
    check_bundle_presence(LAB_REFERENCES, soil_bio_keys, "SOIL BIO / PLFA")

    # ------------------------------------------------------------------
    # SOIL CHEM / HEALTH BUNDLE CHECK
    # ------------------------------------------------------------------
    check_bundle_presence(LAB_REFERENCES, soil_chem_keys, "SOIL CHEM / HEALTH")

    # ------------------------------------------------------------------
    # BASIC REGRESSION CHECK
    # ------------------------------------------------------------------
    print("\n--- REGRESSION CHECK ---")

    for key in [
        "ph",
        "soil_respiration",
        "weoc",
        "crude_protein_pct_db",
        "total_biomass",
        "predator_prey",
        "water_stable_aggregates",
    ]:
        bundle = LAB_REFERENCES.get(key)
        if bundle is None:
            fail(f"{key}: missing")
        else:
            ok(f"{key}: present")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())