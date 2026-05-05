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