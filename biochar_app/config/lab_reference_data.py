# biochar_app/config/lab_reference_data.py
from __future__ import annotations

from biochar_app.config.lab_reference_models import (
    ReferenceInfo,
    InterpretationBand,
    InterpretationInfo,
    VariableReferenceBundle,
)

# ---------------------------------------------------------------------
# Guide URLs / reference landing pages
# ---------------------------------------------------------------------
# These can later be centralized in ward_reference_config.py if desired.
WARD_GUIDE_HTML = "/lab-references/ward-guide"
SHA_GUIDE_HTML = "/lab-references/soil-health-guide"

WARD_GUIDE_PDF = "/lab-references/ward-guide/pdf"
SHA_GUIDE_PDF = "/lab-references/soil-health-guide/pdf"


# ---------------------------------------------------------------------
# Soil chemistry references
# ---------------------------------------------------------------------

PHOSPHORUS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Phosphorus supports plant energy transfer, root development, and early growth."
    ),
    detail=(
        "WardGuide describes phosphorus as a core part of ATP and related energy-transfer "
        "compounds, as well as DNA, RNA, and membrane phospholipids. Because of its role in "
        "energy transfer and growth, phosphorus is especially important during establishment "
        "and active plant development."
    ),
    interpretation=(
        "Interpret phosphorus using the extraction method and the appropriate WardGuide "
        "sufficiency table. Changes over time are most meaningful when evaluated relative "
        "to sufficiency categories, not just raw ppm values."
    ),
    caveat=(
        "Do not assume phosphorus values from different extraction methods are directly "
        "interchangeable. Use the matching WardGuide table for the method reported by the lab."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Phosphorus",
            anchor="soil-phosphorus",
            page_hint=87,
            source_url=f"{WARD_GUIDE_HTML}#soil-phosphorus",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 18",
            table_title="Sufficiency Ranges for Phosphorus Soil Tests",
            page_hint=38,
            source_url=f"{WARD_GUIDE_HTML}#table-18",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 60",
            table_title="Phosphorus Sufficiency Levels for Mehlich P-3, Bray P-1, and Olsen P Soil Tests",
            page_hint=99,
            source_url=f"{WARD_GUIDE_HTML}#table-60",
        ),
    ),
    thresholds=None,  # TODO: add exact method-specific bands once the final Ward table values are confirmed
)

POTASSIUM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Potassium helps regulate water relations, osmotic control, enzyme activity, and photosynthesis."
    ),
    detail=(
        "WardGuide describes potassium as highly mobile in the plant and important for "
        "osmotic control, stomatal function, enzyme activation, and support of growth through "
        "cell expansion and photosynthetic function."
    ),
    interpretation=(
        "Interpret potassium using the appropriate WardGuide sufficiency table and in the "
        "context of crop type, soil texture, and other cation relationships."
    ),
    caveat=(
        "High potassium is not always a simple benefit. Interpretation should consider plant "
        "needs and possible interactions with other nutrients such as magnesium."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Potassium",
            anchor="soil-potassium",
            page_hint=89,
            source_url=f"{WARD_GUIDE_HTML}#soil-potassium",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 21",
            table_title="Sufficiency Ranges for Soil Potassium Test",
            page_hint=40,
            source_url=f"{WARD_GUIDE_HTML}#table-21",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 61",
            table_title="Percent Sufficiency of Soil K Tests",
            page_hint=100,
            source_url=f"{WARD_GUIDE_HTML}#table-61",
        ),
    ),
    thresholds=None,  # TODO: add exact K bands once the final Ward table values are confirmed
)

ORGANIC_MATTER_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Organic matter is a key soil property related to nutrient supply, structure, and biological activity."
    ),
    detail=(
        "WardGuide includes organic matter as a core soil property and connects it to soil "
        "function, nutrient cycling, and broader fertility interpretation."
    ),
    interpretation=(
        "Organic matter is most useful when tracked over time and interpreted alongside "
        "biological activity, aggregation, and nutrient dynamics."
    ),
    caveat=(
        "A higher organic matter value is often favorable, but the practical meaning depends "
        "on soil type, climate, management, and whether biologically active fractions are also improving."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Organic Matter",
            anchor="organic-matter",
            page_hint=77,
            source_url=f"{WARD_GUIDE_HTML}#organic-matter",
        ),
    ),
)

CEC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Cation exchange capacity reflects the soil’s capacity to retain and exchange positively charged nutrients."
    ),
    detail=(
        "WardGuide treats CEC as a core soil property connected to soil texture, nutrient holding capacity, "
        "and fertility interpretation."
    ),
    interpretation=(
        "CEC is best interpreted together with soil texture, pH, organic matter, and base cation balance."
    ),
    caveat=(
        "CEC is not a stand-alone fertility score. The meaning of a given value depends strongly on soil texture."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Cation Exchange Capacity",
            anchor="cation-exchange-capacity",
            page_hint=74,
            source_url=f"{WARD_GUIDE_HTML}#cation-exchange-capacity",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 36",
            table_title="CEC Ranges for Different Soil Textures, pH < 7.0",
            page_hint=55,
            source_url=f"{WARD_GUIDE_HTML}#table-36",
        ),
    ),
)

PH_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soil pH affects nutrient availability, microbial processes, and overall fertility interpretation."
    ),
    detail=(
        "WardGuide includes principles of soil pH and its effects on nutrient behavior and crop response."
    ),
    interpretation=(
        "Interpret pH in relation to crop needs and expected nutrient availability, not as an isolated value."
    ),
    caveat=(
        "The same pH value can have different practical implications depending on crop, soil chemistry, "
        "and whether liming or salinity issues are also present."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Principles of Soil pH",
            anchor="principles-of-soil-ph",
            page_hint=81,
            source_url=f"{WARD_GUIDE_HTML}#principles-of-soil-ph",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 37",
            table_title="1:1 pH Rating",
            page_hint=55,
            source_url=f"{WARD_GUIDE_HTML}#table-37",
        ),
    ),
)


# ---------------------------------------------------------------------
# Soil biology / soil health references
# ---------------------------------------------------------------------

SOIL_RESPIRATION_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soil respiration measures microbial CO2 production after rewetting and reflects biological activity."
    ),
    detail=(
        "The SHA guide explains soil respiration as the amount of CO2-C produced in a 24-hour "
        "incubation following a drying and rewetting event. It is used as an indicator of "
        "microbial activity and relates to nutrient cycling, residue decomposition, aggregation, "
        "and other healthy-soil functions."
    ),
    interpretation=(
        "Higher respiration generally indicates greater microbial activity, but values should be "
        "interpreted relative to climate, management system, crop diversity, and soil type."
    ),
    caveat=(
        "Do not treat respiration as a universal stand-alone score. Compare within similar "
        "soil, climate, and management contexts."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Soil Respiration",
            anchor="soil-respiration",
            page_hint=1,
            source_url=f"{SHA_GUIDE_HTML}#soil-respiration",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 66",
            table_title="Soil Respiration Ranking Table",
            page_hint=113,
            source_url=f"{WARD_GUIDE_HTML}#table-66",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ppm CO2-C",
        method_note="Ward SHA respiration categories.",
        bands=(
            InterpretationBand(label="Low", min_value=0, max_value=60),
            InterpretationBand(label="Marginal", min_value=60, max_value=120),
            InterpretationBand(label="Optimal", min_value=120, max_value=None),
        ),
    ),
)

WEOC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Water extractable organic carbon (WEOC) estimates the readily available carbon food source for microbes."
    ),
    detail=(
        "The SHA guide describes WEOC as the smaller, more available carbon pool that helps "
        "indicate the quality of organic matter from a biological perspective. It reflects food "
        "available to microbes and is linked to root exudates, residue breakdown, and cycling."
    ),
    interpretation=(
        "Higher WEOC generally indicates more readily available carbon for microbial use, but "
        "season, crop growth stage, temperature, and management can strongly affect values."
    ),
    caveat=(
        "WEOC should be interpreted together with respiration, WEON, and broader system context, "
        "not as a stand-alone fertility number."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Water Extractable Organic Carbon",
            anchor="water-extractable-organic-carbon",
            page_hint=2,
            source_url=f"{SHA_GUIDE_HTML}#water-extractable-organic-carbon",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ppm",
        method_note="Ward SHA WEOC categories.",
        bands=(
            InterpretationBand(label="Low", min_value=0, max_value=120),
            InterpretationBand(label="Marginal", min_value=120, max_value=240),
            InterpretationBand(label="Optimal", min_value=240, max_value=None),
        ),
    ),
)

WEON_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Water extractable organic nitrogen (WEON) estimates the pool of organic nitrogen available to microbes."
    ),
    detail=(
        "The SHA guide describes WEON as the pool of organic nitrogen linked to microbial food "
        "sources and biological nitrogen cycling. It is part of the biologically active nutrient pool."
    ),
    interpretation=(
        "Higher WEON generally suggests a larger biologically active organic nitrogen pool, "
        "especially when balanced with available carbon."
    ),
    caveat=(
        "WEON is most informative when interpreted alongside WEOC, respiration, and the water-extractable C:N ratio."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Water Extractable Organic Nitrogen",
            anchor="water-extractable-organic-nitrogen",
            page_hint=3,
            source_url=f"{SHA_GUIDE_HTML}#water-extractable-organic-nitrogen",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ppm",
        method_note="Ward SHA WEON categories.",
        bands=(
            InterpretationBand(label="Low", min_value=0, max_value=12),
            InterpretationBand(label="Marginal", min_value=12, max_value=25),
            InterpretationBand(label="Optimal", min_value=25, max_value=None),
        ),
    ),
)

MAC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Microbially active carbon (%MAC) estimates how much of the WEOC pool is being acted on by microbes."
    ),
    detail=(
        "The SHA guide defines %MAC as the share of the WEOC pool represented by microbial activity "
        "as measured through respiration. It is intended to show whether WEOC may be limiting or balanced."
    ),
    interpretation=(
        "Values between 50% and 75% are described by Ward as a good balance for many production systems. "
        "Very low values may suggest other constraints on microbial activity; very high values may suggest "
        "WEOC could soon become limiting."
    ),
    caveat=(
        "Interpret %MAC together with respiration and WEOC, not by itself."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Microbially Active Carbon (%MAC)",
            anchor="microbially-active-carbon",
            page_hint=3,
            source_url=f"{SHA_GUIDE_HTML}#microbially-active-carbon",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="%",
        method_note="Ward SHA %MAC categories.",
        bands=(
            InterpretationBand(label="Low", min_value=0, max_value=20),
            InterpretationBand(label="Medium", min_value=20, max_value=80),
            InterpretationBand(label="High", min_value=80, max_value=None),
        ),
    ),
)

SOIL_HEALTH_SCORE_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The soil health score is a composite SHA indicator built from respiration, WEOC, WEON, and water-extractable C:N."
    ),
    detail=(
        "The SHA guide describes the soil health score as a quick reference intended to compare "
        "management systems and soils, while emphasizing that it must still be interpreted within "
        "regional and system context."
    ),
    interpretation=(
        "Ward notes that scores above 11 are a useful starting point, but the score is most meaningful "
        "when comparing similar soils and management systems within a climatic region."
    ),
    caveat=(
        "Do not use the composite score alone as a final judgment. The underlying indicators still matter."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Soil Health Score",
            anchor="soil-health-score",
            page_hint=7,
            source_url=f"{SHA_GUIDE_HTML}#soil-health-score",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="score",
        method_note="Ward SHA soil health score categories.",
        bands=(
            InterpretationBand(label="Low", min_value=0, max_value=10),
            InterpretationBand(label="Medium", min_value=10, max_value=40),
            InterpretationBand(label="High", min_value=40, max_value=None),
        ),
    ),
)

ORGANIC_CN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The water-extractable organic C:N ratio reflects the balance between microbial energy and nutrition."
    ),
    detail=(
        "The SHA guide explains this as the balance between water-extractable organic carbon and "
        "water-extractable organic nitrogen, emphasizing that it is not the same as total soil C:N "
        "or the C:N ratio of manure, residue, or bulk soil organic matter."
    ),
    interpretation=(
        "The ratio is intended to help interpret whether the biologically active pool is balanced "
        "for microbial function."
    ),
    caveat=(
        "This is a specific water-extractable C:N ratio from the SHA and should not be confused with "
        "bulk soil or residue C:N ratios."
    ),
    references=(
        ReferenceInfo(
            guide_key="sha_guide",
            guide_label="Soil Health Assessment Guide",
            section_title="Organic C to Organic N Ratio",
            anchor="organic-c-to-organic-n-ratio",
            page_hint=4,
            source_url=f"{SHA_GUIDE_HTML}#organic-c-to-organic-n-ratio",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 68",
            table_title="Organic C:N Ration Ranking Table",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#table-68",
        ),
    ),
    thresholds=None,  # TODO: add exact bands after confirming the final WardGuide table values
)


# ---------------------------------------------------------------------
# NIR / forage references
# ---------------------------------------------------------------------

CRUDE_PROTEIN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Crude protein estimates the total protein-related nitrogen content of the forage."
    ),
    detail=(
        "WardGuide places crude protein within the core nutrient feed tests used to evaluate "
        "feed value and ration quality."
    ),
    interpretation=(
        "Interpret crude protein in the context of forage type, animal class, and overall ration balance."
    ),
    caveat=(
        "Crude protein is informative but should not be read by itself without fiber, digestibility, "
        "and energy context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Nutrient Feed Tests",
            anchor="nutrient-feed-tests",
            page_hint=19,
            source_url=f"{WARD_GUIDE_HTML}#nutrient-feed-tests",
        ),
    ),
)

RFV_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Relative Feed Value (RFV) is an index used to compare forage quality."
    ),
    detail=(
        "WardGuide includes forage quality indexes and presents RFV as one of the summary metrics "
        "used to compare feed value."
    ),
    interpretation=(
        "Higher RFV generally indicates higher quality forage, but interpretation still depends on "
        "animal type and production goals."
    ),
    caveat=(
        "RFV is a comparative index and should not replace full ration interpretation."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Forage Quality Indexes",
            anchor="forage-quality-indexes",
            page_hint=28,
            source_url=f"{WARD_GUIDE_HTML}#forage-quality-indexes",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 7",
            table_title="Relative Forage Quality Suggested for Different Cattle Types",
            page_hint=28,
            source_url=f"{WARD_GUIDE_HTML}#table-7",
        ),
    ),
)

NIRS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Near-Infrared Spectroscopy (NIRS) is used to estimate forage constituents efficiently."
    ),
    detail=(
        "WardGuide includes a dedicated NIRS section and a table distinguishing recommended NIR "
        "tests from tests that require wet chemistry."
    ),
    interpretation=(
        "NIRS is useful for rapid estimation of many forage traits, but some analytes still require "
        "wet chemistry depending on the testing goal."
    ),
    caveat=(
        "Interpret NIR-derived values with awareness of method limits and when wet chemistry confirmation is needed."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 9",
            table_title="NIR Recommended and Wet Chemistry Required Tests",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#table-9",
        ),
    ),
)

TOTAL_BIOMASS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Total microbial biomass reflects the overall size of the living microbial community "
        "and is a primary indicator of soil biological activity."
    ),
    detail=(
        "WardGuide explains that phospholipid fatty acids (PLFAs) are found in the membranes "
        "of active organisms and that quantifying these compounds can be used to estimate both "
        "the total living microbial biomass and the size of specific microbial groups. In this "
        "context, total biomass represents the overall living microbial community measured by PLFA."
    ),
    interpretation=(
        "Higher total biomass often suggests a larger active microbial community and can indicate "
        "greater biological activity, residue processing potential, and nutrient cycling capacity. "
        "WardGuide also notes that total microbial biomass can be tracked over time as soil health changes."
    ),
    caveat=(
        "Interpret total biomass comparatively across treatments, dates, and management systems. "
        "A higher value is not automatically better in every context, because moisture, residue inputs, "
        "crop stage, and seasonal conditions can all influence the measurement."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Microorganisms",
            anchor="soil-microorganisms",
            page_hint=79,
            source_url=f"{WARD_GUIDE_HTML}#soil-microorganisms",
        ),
    ),
)

BACTERIA_BIOMASS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Bacterial biomass represents the living bacterial share of the soil microbial community."
    ),
    detail=(
        "WardGuide states that certain PLFAs can be used to indicate specific microbial groups, "
        "including bacteria. In this table, bacterial biomass refers to the portion of the living "
        "PLFA-measured community attributed to bacteria."
    ),
    interpretation=(
        "Higher bacterial biomass can reflect stronger bacterial activity in decomposition and nutrient "
        "cycling, especially in systems with more readily processed organic inputs. It can be useful for "
        "tracking how management, amendment, or season shifts the structure of the living microbial community."
    ),
    caveat=(
        "Bacterial biomass is best interpreted together with total biomass, fungal biomass, and treatment "
        "history. A larger bacterial pool is not inherently better or worse without the broader biological context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Microorganisms",
            anchor="soil-microorganisms",
            page_hint=79,
            source_url=f"{WARD_GUIDE_HTML}#soil-microorganisms",
        ),
    ),
)

FUNGI_BIOMASS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Fungal biomass represents the living fungal share of the soil microbial community."
    ),
    detail=(
        "WardGuide explains that specific PLFAs can indicate fungi as one of the major microbial groups. "
        "In this table, fungal biomass refers to the PLFA-measured living fungal component of the soil community."
    ),
    interpretation=(
        "Higher fungal biomass can be associated with stronger residue decomposition of more complex organic materials, "
        "greater contribution to soil aggregation, and a more fungal-influenced community structure."
    ),
    caveat=(
        "Interpret fungal biomass comparatively across treatments and dates. It is most informative when viewed "
        "alongside bacterial biomass, total biomass, and management conditions rather than as a stand-alone target."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Microorganisms",
            anchor="soil-microorganisms",
            page_hint=79,
            source_url=f"{WARD_GUIDE_HTML}#soil-microorganisms",
        ),
    ),
)

FUNGI_BACTERIA_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The fungi-to-bacteria ratio compares the relative balance of fungal and bacterial biomass."
    ),
    detail=(
        "WardGuide explains that PLFA can estimate the size of bacterial and fungal groups within the living "
        "soil microbial community. The fungi:bacteria value shown here is a derived comparative ratio based on "
        "those two measured pools."
    ),
    interpretation=(
        "Higher values indicate relatively greater fungal representation compared with bacteria, while lower values "
        "indicate relatively greater bacterial representation. This can be useful for comparing treatment effects, "
        "changes over time, and broad shifts in community structure."
    ),
    caveat=(
        "WardGuide supports interpretation of fungal and bacterial groups, but this ratio should still be treated "
        "as a comparative derived metric rather than a universal stand-alone target. Use it with total biomass, "
        "management context, and other soil health indicators."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Microorganisms",
            anchor="soil-microorganisms",
            page_hint=79,
            source_url=f"{WARD_GUIDE_HTML}#soil-microorganisms",
        ),
    ),
)

MYCORRHIZAE_BIOMASS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Mycorrhizae biomass reflects the PLFA-measured mycorrhizal component of the soil microbial community."
    ),
    detail=(
        "WardGuide’s PLFA discussion explains that specific PLFAs can be used to indicate distinct microbial groups. "
        "This variable represents the mycorrhizal portion of that measured living community."
    ),
    interpretation=(
        "Higher mycorrhizae biomass can suggest stronger root-fungal association and greater potential for biologically "
        "mediated nutrient uptake, especially where plant roots and fungal symbiosis are active."
    ),
    caveat=(
        "The uploaded Ward material provides stronger general support for PLFA group interpretation than for a long, "
        "stand-alone explanation of mycorrhizae biomass specifically. Treat this variable as a useful comparative indicator, "
        "best interpreted alongside crop type, season, root activity, and other biological measurements."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
    ),
)

# ---------------------------------------------------------------------
# Master lookup
# ---------------------------------------------------------------------

LAB_REFERENCES = {
    # Soil chemistry
    "phosphorus": PHOSPHORUS_REFERENCE,
    "potassium": POTASSIUM_REFERENCE,
    "organic_matter": ORGANIC_MATTER_REFERENCE,
    "cec": CEC_REFERENCE,
    "ph": PH_REFERENCE,

    # Soil biology / soil health
    "soil_respiration": SOIL_RESPIRATION_REFERENCE,
    "weoc": WEOC_REFERENCE,
    "weon": WEON_REFERENCE,
    "mac": MAC_REFERENCE,
    "soil_health_score": SOIL_HEALTH_SCORE_REFERENCE,
    "organic_cn": ORGANIC_CN_REFERENCE,

    # Soil biomass
    "total_biomass": TOTAL_BIOMASS_REFERENCE,
    "bacteria_biomass": BACTERIA_BIOMASS_REFERENCE,
    "fungi_biomass": FUNGI_BIOMASS_REFERENCE,
    "fungi_bacteria": FUNGI_BACTERIA_REFERENCE,
    "mycorrhizae_biomass": MYCORRHIZAE_BIOMASS_REFERENCE,

    # NIR / forage
    "crude_protein_pct_db": CRUDE_PROTEIN_REFERENCE,
    "rfv": RFV_REFERENCE,
    "nirs": NIRS_REFERENCE,
}