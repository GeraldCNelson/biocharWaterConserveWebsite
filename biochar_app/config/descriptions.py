# biochar_app/config/descriptions.py

# ---------------------------------------------------------------------
# Project identity
# ---------------------------------------------------------------------

PROJECT_NAME = "Biochar Fruita CSU"
PROJECT_README_TITLE = f"{PROJECT_NAME} – Bulk Download"

PROJECT_FIELD_DESCRIPTION = f"{PROJECT_NAME} field experiment"

# ---------------------------------------------------------------------
# Experimental design notes
# ---------------------------------------------------------------------

PROJECT_STRIP_NOTE = (
    "Samples were collected from four strips (S1–S4)."
)

PROJECT_TREATMENT_NOTE = (
    "Biochar was applied to strips S1 and S3 after March 2023 sampling. "
    "Strips S2 and S4 serve as controls."
)

PROJECT_SAMPLE_CONTEXT_NOTE = (
    f"{PROJECT_STRIP_NOTE} {PROJECT_TREATMENT_NOTE}"
)

# ---------------------------------------------------------------------
# External reference sources (project-configurable)
# ---------------------------------------------------------------------

PROJECT_SOIL_HEALTH_OVERVIEW_URL = (
    "https://www.wardlab.com/wp-content/uploads/2024/04/2024-Soil-Health-One-Pager-C.pdf"
)

PROJECT_SOIL_HEALTH_GUIDE_URL = (
    "https://www.wardlab.com/wp-content/uploads/2024/12/SHA-Guide-FINAL-May.pdf"
)

PROJECT_REFERENCE_SOURCES = [
    ("Soil Health Analysis overview", PROJECT_SOIL_HEALTH_OVERVIEW_URL),
    ("Soil Health Assessment Guide", PROJECT_SOIL_HEALTH_GUIDE_URL),
]

PROJECT_METHOD_NOTE = "Lab method; see Soil Health Assessment Guide"

PROJECT_REFERENCE_NOTE = (
    "Several soil chemistry and soil health definitions are derived from project reference "
    "documents. Where those documents describe a measurement method but do not clearly "
    "specify the exported unit, the README reflects the best available interpretation."
)

# ---------------------------------------------------------------------
# Dataset descriptions
# ---------------------------------------------------------------------

LOGGER_DESCRIPTION = (
    f"This file contains standardized logger-derived data for the {PROJECT_FIELD_DESCRIPTION}. "
    "It includes raw logger measurements, derived soil water content values, "
    "treatment-control ratios, and treatment-control differences where available. "
    "Sensors record soil conditions at three depths and three logger locations within each strip."
)

WEATHER_DATA_SOURCE = (
    "CoAgMet station FRT3: https://coagmet.colostate.edu/data/url-builder"
)

WEATHER_DESCRIPTION = (
    f"This file contains standardized weather data from the associated {WEATHER_DATA_SOURCE}, "
    f"aligned with the {PROJECT_FIELD_DESCRIPTION}. These data provide environmental "
    "context for soil and plant measurements, including temperature, precipitation, "
    "and related variables."
)

IRRIGATION_DESCRIPTION = (
    f"This dataset contains standardized irrigation management records for the {PROJECT_FIELD_DESCRIPTION}. "
    "The site uses furrow irrigation. Strips S1 and S2 are irrigated together on the west side, "
    "and strips S3 and S4 are irrigated together on the east side. Irrigation events include "
    "timing, duration, and applied water volume where available."
)

FERTILIZER_DESCRIPTION = (
    f"This dataset contains standardized fertilizer application records for the {PROJECT_FIELD_DESCRIPTION}. "
    "Applications are recorded at the strip level and include timing, material, and application amounts where available."
)

SOIL_CHEMISTRY_DESCRIPTION = (
    f"This dataset contains laboratory soil chemistry measurements collected from the {PROJECT_FIELD_DESCRIPTION}."
)

SOIL_CHEMISTRY_SCOPE_NOTE = (
    "The dataset includes measurements of soil nutrients, pH, salinity, organic matter, "
    "soil organic carbon, and related chemical properties used to evaluate soil fertility "
    "and carbon dynamics."
)

SOIL_BIOLOGY_DESCRIPTION = (
    f"This dataset contains laboratory soil biology measurements collected from the {PROJECT_FIELD_DESCRIPTION}."
)

SOIL_BIOLOGY_SCOPE_NOTE = (
    "These data are used to evaluate microbial biomass, microbial community structure, "
    "and biological response to biochar treatment."
)

HAY_DESCRIPTION = (
    f"This dataset contains biomass, hay, and forage quality measurements from the {PROJECT_FIELD_DESCRIPTION}. "
    "These data are used to evaluate plant production, forage quality, nutrient removal, "
    "and crop response to biochar treatment."
)

HAY_SAMPLE_CONTEXT_NOTE = (
    f"Samples are associated with the four experimental strips (S1–S4). {PROJECT_TREATMENT_NOTE}"
)

GENERIC_FILE_DESCRIPTION = (
    f"This archive contains a standardized all-years dataset from the {PROJECT_NAME} project."
)

PROJECT_NIR_REFERENCE_SOURCES = [
    ("NIRS Consortium", "https://www.nirsconsortium.com/"),
    ("NIRS Consortium Wet Chemistry Primary Methods", "NIRSC Guidelines Document Primary Methods.pdf"),
]

PROJECT_NIR_REFERENCE_NOTE = (
    "Hay and forage-quality variable descriptions and units are informed by "
    "NIRS Consortium wet-chemistry primary-method guidance where applicable."
)