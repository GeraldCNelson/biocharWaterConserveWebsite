#!/usr/bin/env python3
from __future__ import annotations

from biochar_app.config.lab_reference_data import LAB_REFERENCES
from biochar_app.scripts.tables.tables_soil_bio import SOILBIO_VARIABLE_GROUPS


def main() -> None:
    print("✅ Imported successfully")
    print(f"✅ LAB_REFERENCES size: {len(LAB_REFERENCES)}")

    print("\n--- SOIL BIO REFERENCE CHECK ---")

    missing_keys: list[tuple[str, str, str, str]] = []
    none_keys: list[tuple[str, str, str]] = []

    for group in SOILBIO_VARIABLE_GROUPS:
        group_key = group["group_key"]
        group_label = group["group_label"]
        variables = group["variables"]

        print(f"\n[{group_key}] {group_label}")

        for var in variables:
            ref_key = var.reference_key
            print(f"  {var.key}: reference_key={ref_key}")

            if ref_key is None:
                none_keys.append((group_key, var.key, var.label))
                continue

            ref_bundle = LAB_REFERENCES.get(ref_key)
            if ref_bundle is None:
                print("    ❌ MISSING from LAB_REFERENCES")
                missing_keys.append((group_key, var.key, var.label, ref_key))
                continue

            first_ref = ref_bundle.references[0].section_title if ref_bundle.references else "(no references)"
            print(f"    ✅ OK -> {first_ref}")

    print("\n--- SUMMARY ---")
    if missing_keys:
        print("❌ Missing non-None reference_key values:")
        for group_key, var_key, label, ref_key in missing_keys:
            print(f"  - {group_key} / {var_key} / {label} / reference_key={ref_key}")
    else:
        print("✅ All non-None reference_key values exist in LAB_REFERENCES")

    print("\nVariables still using reference_key=None:")
    if none_keys:
        for group_key, var_key, label in none_keys:
            print(f"  - {group_key} / {var_key} / {label}")
    else:
        print("  (none)")


if __name__ == "__main__":
    main()