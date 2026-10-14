# biochar_app/config/fertilizer.py

"""
Fertilizer product analysis (elemental mass fraction basis)

All values are expressed as fractions (e.g., 0.11 = 11%).

Important:
- N, P, K, Mn, and S are treated as elemental mass fractions.
- Fertilizer labels usually report phosphorus as P2O5 and potassium as K2O.
- This file converts those label values to elemental P and elemental K.

Conversions:
- elemental P = P2O5 * 0.4364
- elemental K = K2O * 0.8301

# Add additional micronutrients here only when they appear in fertilizer source data
# or become part of the project nutrient budget, e.g.:
# "fe", "zn", "cu", "b", "mo", "cl", "ni"
"""

P_FROM_P2O5 = 0.4364
K_FROM_K2O = 0.8301

# Product-grade fractions as printed on the fertilizer label.  These are used
# to translate a nutrient recommendation into a product rate.  P and K are
# label-basis P2O5 and K2O here; FERTILIZER_PRODUCT_ANALYSIS below converts
# those values to elemental mass fractions for nutrient-budget columns.
FERTILIZER_PRODUCT_GRADE = {
    "11-52-0": {"n": 0.11, "p2o5": 0.52, "k2o": 0.00},
    "UREA": {"n": 0.46},
    "SULFATE OF POTASH": {"k2o": 0.50},
    "MANGANESE SULFATE (MNSO4)": {"mn": 0.32},
}

# The annual recommendation sheets use several names for the same products.
# The 2024-2026 row labelled "11-52-1" states "11, 52, 0%" in its analysis
# cell, so it is mapped to 11-52-0 pending confirmation of the actual product.
FERTILIZER_PRODUCT_ALIASES = {
    "11-52-0": "11-52-0",
    "LBS OF 11-52-0": "11-52-0",
    "11-52-1 (PHOSPHORUS)": "11-52-0",
    "UREA": "UREA",
    "LBS OF UREA": "UREA",
    "46-0-0 UREA (N)": "UREA",
    "SULFATE OF POTASH": "SULFATE OF POTASH",
    "LBS OF SULFATE OF POTASH": "SULFATE OF POTASH",
    "POTASSIUM SULFATE (K2SO4)": "SULFATE OF POTASH",
    "MANGANESE SULFATE (MNSO4)": "MANGANESE SULFATE (MNSO4)",
}

FERTILIZER_PRODUCT_ANALYSIS = {
    # 11-52-0 = 11% N, 52% P2O5, 0% K2O
    "11-52-0": {
        "n": 0.11,
        "p": 0.52 * P_FROM_P2O5,
        "k": 0.00,
        "mn": 0.00,
        "s": 0.00,
    },

    # Urea = 46% elemental N
    "UREA": {
        "n": 0.46,
        "p": 0.00,
        "k": 0.00,
        "mn": 0.00,
        "s": 0.00,
    },

    # Sulfate of potash = commonly 0-0-50, so 50% K2O
    "SULFATE OF POTASH": {
        "n": 0.00,
        "p": 0.00,
        "k": 0.50 * K_FROM_K2O,
        "mn": 0.00,
        "s": 0.18,
    },

    # Manganese sulfate; Mn and S fractions vary slightly by formulation/hydration.
    "MANGANESE SULFATE (MNSO4)": {
        "n": 0.00,
        "p": 0.00,
        "k": 0.00,
        "mn": 0.32,
        "s": 0.185,
    },
}
