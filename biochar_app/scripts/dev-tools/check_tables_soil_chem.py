#!/usr/bin/env python3
from __future__ import annotations

from biochar_app.config.lab_reference_data import LAB_REFERENCES
from biochar_app.scripts.tables.tables_soil_chem import SOILCHEM_VARIABLE_GROUPS


def main() -> None:
    print("✅ Imported successfully")
    print(f"✅ LAB_REFERENCES size: {len(LAB_REFERENCES)}")
    print()

    missing_reference_keys: list[tuple[str, str, str]] = []
    none_reference_keys: list[tuple[str, str, str]] = []

    print("--- SOIL CHEM REFERENCE CHECK ---")
    for group in SOILCHEM_VARIABLE_GROUPS:
        group_key = group["group_key"]
        group_label = group["group_label"]
        print(f"\n[{group_key}] {group_label}")

        for var in group["variables"]:
            ref_key = var.reference_key
            print(f"  {var.key}: reference_key={ref_key}")

            if ref_key is None:
                none_reference_keys.append((group_key, var.key, var.label))
                continue

            if ref_key not in LAB_REFERENCES:
                missing_reference_keys.append((group_key, var.key, ref_key))
                print(f"    ❌ Missing in LAB_REFERENCES: {ref_key}")
            else:
                bundle = LAB_REFERENCES[ref_key]
                if bundle is None:
                    print("    ⚠️ Present but mapped to None")
                else:
                    titles = [
                        ref.section_title or ref.table_title or ref.guide_label
                        for ref in bundle.references
                    ]
                    first_title = titles[0] if titles else "(no reference titles)"
                    print(f"    ✅ OK -> {first_title}")

    print("\n--- SUMMARY ---")
    if missing_reference_keys:
        print("❌ Missing LAB_REFERENCES entries:")
        for group_key, var_key, ref_key in missing_reference_keys:
            print(f"  - {group_key} / {var_key} -> {ref_key}")
    else:
        print("✅ All non-None reference_key values exist in LAB_REFERENCES")

    print("\nVariables still using reference_key=None:")
    for group_key, var_key, label in none_reference_keys:
        print(f"  - {group_key} / {var_key} / {label}")


if __name__ == "__main__":
    main()