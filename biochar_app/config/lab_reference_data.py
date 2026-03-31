from __future__ import annotations

from biochar_app.config.lab_reference_models import (
    ReferenceInfo,
    InterpretationBand,
    InterpretationInfo,
    VariableReferenceBundle,
)


def combine_reference_bundles(
    *bundles: VariableReferenceBundle,
) -> VariableReferenceBundle:
    references = []
    seen = set()

    for bundle in bundles:
        for ref in bundle.references:
            key = (
                ref.guide_key,
                ref.section_title,
                ref.table_title,
                ref.anchor,
                ref.source_url,
            )
            if key not in seen:
                seen.add(key)
                references.append(ref)

    first = bundles[0]

    return VariableReferenceBundle(
        short_note=first.short_note,
        detail=first.detail,
        interpretation=first.interpretation,
        caveat=first.caveat,
        references=tuple(references),
        thresholds=first.thresholds,
    )


# ---------------------------------------------------------------------
# Guide URLs / reference landing pages
# ---------------------------------------------------------------------
WARD_GUIDE_HTML = "/lab-references/ward-guide"
SHA_GUIDE_HTML = "/lab-references/soil-health-guide"
WARD_BIOLOGICAL_REPORT_HTML = "/lab-references/ward-biological-report"

WARD_GUIDE_PDF = "/lab-references/ward-guide/pdf"
SHA_GUIDE_PDF = "/lab-references/soil-health-guide/pdf"
WARD_BIOLOGICAL_REPORT_PDF = "/lab-references/ward-biological-report/pdf"

WARD_BIOLOGICAL_REPORT_DATE = "2025-11-05"
WARD_BIOLOGICAL_REPORT_LABEL = f"Ward Biological Report ({WARD_BIOLOGICAL_REPORT_DATE})"

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
            source_url=f"{WARD_GUIDE_HTML}#table-18-sufficiency-ranges-for-phosphorus-soil-tests",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 60",
            table_title=(
                "Phosphorus Sufficiency Levels for Mehlich P-3, Bray P-1, "
                "and Olsen P Soil Tests"
            ),
            page_hint=99,
            source_url=f"{WARD_GUIDE_HTML}#table-60-phosphorus-sufficiency-levels-for-mehlich-p-3-bray-p-1-and-olsen-p-soil-tests",
        ),
    ),
    thresholds=None,
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
            source_url=f"{WARD_GUIDE_HTML}#table-21-sufficiency-ranges-for-soil-potassium-test",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 61",
            table_title="Percent Sufficiency of Soil K Tests",
            page_hint=100,
            source_url=f"{WARD_GUIDE_HTML}#table-61-percent-sufficiency-of-soil-k-tests",
        ),
    ),
    thresholds=None,
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
            source_url=f"{WARD_GUIDE_HTML}#table-37-11-ph-rating",
        ),
    ),
)

BUFFER_PH_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Buffer pH helps estimate reserve acidity and supports lime recommendation decisions."
    ),
    detail=(
        "WardGuide explains that a buffer pH measurement is used with soil pH to estimate reserve acidity "
        "held on clays and organic matter, which supports liming recommendations."
    ),
    interpretation=(
        "Interpret buffer pH together with soil pH, crop type, and lime program rather than as an isolated value."
    ),
    caveat=(
        "Buffer pH is a recommendation support measurement, not a direct substitute for soil pH."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Lime Recommendations",
            anchor="lime-recommendations",
            page_hint=48,
            source_url=f"{WARD_GUIDE_HTML}#lime-recommendations",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 30",
            table_title="Lime Recommendation Based on Buffer pH",
            page_hint=49,
            source_url=f"{WARD_GUIDE_HTML}#table-30-lime-recommendation-based-on-buffer-ph",
        ),
    ),
)

SALINITY_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soluble salts and salinity are commonly interpreted from electrical conductivity measurements."
    ),
    detail=(
        "WardGuide explains that salt-affected soils are influenced by soluble salts in the soil solution, "
        "and that salinity is commonly measured by electrical conductivity. For routine soil test reports, "
        "soluble salt interpretations are often tied to 1:1 soil:water measurements."
    ),
    interpretation=(
        "Interpret EC or soluble salts in context. The same measured salinity can have different practical "
        "consequences depending on crop sensitivity, leaching, irrigation water quality, and whether sodium "
        "hazards are also present."
    ),
    caveat=(
        "Do not treat salinity as a stand-alone issue. WardGuide distinguishes among soluble salts, salinity, "
        "sodicity, and irrigation-water hazards, which can overlap but are not identical."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Salt Affected Soil",
            anchor="salt-affected-soil",
            page_hint=104,
            source_url=f"{WARD_GUIDE_HTML}#salt-affected-soil",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 38",
            table_title="Soluble Salt Ratings",
            page_hint=55,
            source_url=f"{WARD_GUIDE_HTML}#table-38-soluble-salt-ratings",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 65",
            table_title="The Relationship Between Conductivity and Degree of Salinity",
            page_hint=105,
            source_url=f"{WARD_GUIDE_HTML}#table-65-the-relationship-between-conductivity-and-degree-of-salinity",
        ),
    ),
)

EXCESS_LIME_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Excess lime affects soil chemistry, nutrient availability, and how pH-related recommendations should be interpreted."
    ),
    detail=(
        "WardGuide discusses liming, buffer pH, and carbonate effects on soil chemistry, while also noting that "
        "free lime or calcareous conditions can influence nutrient availability and the behavior of several soil tests."
    ),
    interpretation=(
        "Treat excess lime as an important contextual factor, especially when interpreting phosphorus, iron, zinc, "
        "manganese, and other nutrients that may behave differently in calcareous soils."
    ),
    caveat=(
        "Excess lime is not simply the same thing as a high pH reading. Its practical meaning depends on carbonate "
        "content, crop sensitivity, and how the lab’s extraction methods respond under calcareous conditions."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Lime Recommendations",
            anchor="lime-recommendations",
            page_hint=48,
            source_url=f"{WARD_GUIDE_HTML}#lime-recommendations",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 30",
            table_title="Lime Recommendation Based on Buffer pH",
            page_hint=49,
            source_url=f"{WARD_GUIDE_HTML}#table-30-lime-recommendation-based-on-buffer-ph",
        ),
    ),
)

NITRATE_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soil nitrate is the mobile, plant-available nitrogen pool most commonly used in fertilizer recommendation work."
    ),
    detail=(
        "WardGuide describes nitrate as the end product of much of the soil nitrogen cycle and emphasizes that it is "
        "water-soluble, mobile, and often measured to estimate nitrogen carryover for the next crop."
    ),
    interpretation=(
        "Interpret nitrate in relation to sampling depth, crop yield goal, subsoil nitrate, manure history, irrigation, "
        "and leaching potential."
    ),
    caveat=(
        "WardGuide notes several limitations of the nitrate test, including that it does not directly measure recent "
        "ammonium from anhydrous ammonia, nitrogen from legumes, or manure mineralization that has not yet occurred."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Nitrate",
            anchor="soil-test-methods-nitrate",
            page_hint=97,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-nitrate",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Nitrogen and Sulfur Fertilizer Recommendation Calculations",
            anchor="nitrogen-and-sulfur-fertilizer-recommendation-calculations",
            page_hint=55,
            source_url=f"{WARD_GUIDE_HTML}#nitrogen-and-sulfur-fertilizer-recommendation-calculations",
        ),
    ),
)

SULFUR_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Sulfur is an essential nutrient whose availability depends on organic matter, sulfate supply, irrigation water, and leaching."
    ),
    detail=(
        "WardGuide explains that sulfur in soil is strongly tied to organic matter and sulfate dynamics. It emphasizes "
        "that sulfur availability is influenced by soil texture, organic matter mineralization, irrigation water, and "
        "atmospheric contributions."
    ),
    interpretation=(
        "Interpret sulfur in relation to the sulfate test, organic matter, irrigation water sulfur, soil texture, and "
        "cropping system."
    ),
    caveat=(
        "WardGuide notes that sulfur interpretation is more complicated than many other nutrients because multiple "
        "sources can supply crop sulfur, and sulfate is mobile in soil."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Sulfur",
            anchor="soil-sulfur",
            page_hint=91,
            source_url=f"{WARD_GUIDE_HTML}#soil-sulfur",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Sulfur",
            anchor="soil-test-methods-sulfur",
            page_hint=101,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-sulfur",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 24",
            table_title="Sulfur Recommendations for Various Crops",
            page_hint=42,
            source_url=f"{WARD_GUIDE_HTML}#table-24-sulfur-recommendations-for-various-crops",
        ),
    ),
)

CALCIUM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soil calcium is usually interpreted together with pH, liming status, and the broader exchange complex."
    ),
    detail=(
        "WardGuide explains that much soil calcium is present as exchangeable calcium and that interpretation of soil "
        "calcium is strongly tied to pH, liming, carbonate status, and the exchangeable cation pool."
    ),
    interpretation=(
        "Interpret calcium in the broader context of soil pH, liming need, and cation balance rather than treating it "
        "as an isolated sufficiency number in most field settings."
    ),
    caveat=(
        "WardGuide notes that simple Ca:Mg ratios are often overemphasized. The practical significance of calcium values "
        "depends more on pH, exchangeability, and broader soil constraints."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Calcium",
            anchor="calcium",
            page_hint=14,
            source_url=f"{WARD_GUIDE_HTML}#calcium",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Calcium and Magnesium",
            anchor="soil-test-methods-calcium-and-magnesium",
            page_hint=103,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-calcium-and-magnesium",
        ),
    ),
)

MAGNESIUM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soil magnesium is interpreted mainly from exchangeable Mg levels, with additional context from pH and crop sensitivity."
    ),
    detail=(
        "WardGuide explains that magnesium availability is tied to exchangeable Mg and that plant response is most likely "
        "when exchangeable magnesium is low. It also discusses interactions with crop type and liming practices."
    ),
    interpretation=(
        "Interpret magnesium using exchangeable Mg levels, crop sensitivity, and soil context rather than relying heavily "
        "on fixed cation-ratio rules."
    ),
    caveat=(
        "WardGuide specifically notes that Ca:Mg ratios are often less important than the actual exchangeable magnesium level."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Magnesium",
            anchor="magnesium",
            page_hint=14,
            source_url=f"{WARD_GUIDE_HTML}#magnesium",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Calcium and Magnesium",
            anchor="soil-test-methods-calcium-and-magnesium",
            page_hint=103,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-calcium-and-magnesium",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 31",
            table_title="Magnesium Soil Test Ratings",
            page_hint=50,
            source_url=f"{WARD_GUIDE_HTML}#table-31-magnesium-soil-test-ratings",
        ),
    ),
)

SODIUM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Sodium is most important in soil interpretation when it contributes to sodicity, infiltration problems, and salt-affected conditions."
    ),
    detail=(
        "WardGuide explains that sodium becomes especially important in salt-affected soils, where high exchangeable sodium "
        "can disperse clay, reduce permeability, and create structural problems."
    ),
    interpretation=(
        "Interpret sodium alongside salinity, SAR, exchangeable sodium effects, and irrigation-water quality rather than as "
        "a stand-alone soil fertility number."
    ),
    caveat=(
        "High sodium is often more a structural and water-management issue than a conventional nutrient sufficiency issue."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Salt Affected Soil",
            anchor="salt-affected-soil",
            page_hint=104,
            source_url=f"{WARD_GUIDE_HTML}#salt-affected-soil",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 64",
            table_title="Classification of Salt Affected Soils Based on Saturation Extracts",
            page_hint=105,
            source_url=f"{WARD_GUIDE_HTML}#table-64-classification-of-salt-affected-soils-based-on-saturation-extracts",
        ),
    ),
)

ZINC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Zinc is a micronutrient whose availability is strongly influenced by soil pH, crop sensitivity, and soil test level."
    ),
    detail=(
        "WardGuide discusses zinc as a key micronutrient, notes that availability declines with increasing pH, and describes "
        "soil testing and fertilizer recommendations based on DTPA zinc and crop responsiveness."
    ),
    interpretation=(
        "Interpret zinc using both the soil test value and crop sensitivity, especially in calcareous or high-pH soils."
    ),
    caveat=(
        "WardGuide notes that phosphorus interactions, pH, and crop-specific responsiveness all influence how meaningful a zinc test value is."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Zinc",
            anchor="soil-zinc",
            page_hint=93,
            source_url=f"{WARD_GUIDE_HTML}#soil-zinc",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Zinc, Iron, Manganese and Copper",
            anchor="soil-test-methods-zinc-iron-manganese-and-copper",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-zinc-iron-manganese-and-copper",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 62",
            table_title="Zinc, Iron, Manganese and Copper Availability Ratings for Various Crops",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#table-62-zinc-iron-manganese-and-copper-availability-ratings-for-various-crops",
        ),
    ),
)

IRON_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Iron availability is strongly affected by soil pH, excess lime, and crop sensitivity."
    ),
    detail=(
        "WardGuide explains that low soil-test iron is often associated with calcareous, high-pH, low-organic-matter soils "
        "and that iron chlorosis is especially important in sensitive crops."
    ),
    interpretation=(
        "Interpret iron in the context of soil test level, pH, excess lime, crop susceptibility, and observed chlorosis."
    ),
    caveat=(
        "WardGuide notes that iron problems are often closely tied to calcareous conditions and bicarbonate effects, not just total iron content."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Zinc, Iron, Manganese and Copper",
            anchor="soil-test-methods-zinc-iron-manganese-and-copper",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-zinc-iron-manganese-and-copper",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 26",
            table_title="Iron Soil Test Ratings",
            page_hint=45,
            source_url=f"{WARD_GUIDE_HTML}#table-26-iron-soil-test-ratings",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 62",
            table_title="Zinc, Iron, Manganese and Copper Availability Ratings for Various Crops",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#table-62-zinc-iron-manganese-and-copper-availability-ratings-for-various-crops",
        ),
    ),
)

MANGANESE_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Manganese availability varies with pH, drainage, organic matter, and redox conditions."
    ),
    detail=(
        "WardGuide explains that manganese availability is influenced by soil pH, wetness, drainage, and organic matter, "
        "and that both deficiency and excess conditions can occur depending on soil environment."
    ),
    interpretation=(
        "Interpret manganese using soil test level, crop sensitivity, and site conditions such as alkalinity, waterlogging, and organic matter."
    ),
    caveat=(
        "WardGuide notes that manganese is especially sensitive to redox and environmental conditions, so a soil test should not be interpreted in isolation."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Zinc, Iron, Manganese and Copper",
            anchor="soil-test-methods-zinc-iron-manganese-and-copper",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-zinc-iron-manganese-and-copper",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 28",
            table_title="Manganese Fertilizer Recommendations for Various Crops",
            page_hint=47,
            source_url=f"{WARD_GUIDE_HTML}#table-28-manganese-fertilizer-recommendations-for-various-crops",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 62",
            table_title="Zinc, Iron, Manganese and Copper Availability Ratings for Various Crops",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#table-62-zinc-iron-manganese-and-copper-availability-ratings-for-various-crops",
        ),
    ),
)

COPPER_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Copper is a micronutrient most often interpreted in relation to deficiency risk, soil type, and DTPA copper levels."
    ),
    detail=(
        "WardGuide explains that copper deficiency is less common than some other micronutrient deficiencies, but it may occur "
        "in certain sandy, organic, acid, or no-till settings."
    ),
    interpretation=(
        "Interpret copper using soil test level, site context, and crop sensitivity rather than assuming a broad deficiency or adequacy rule."
    ),
    caveat=(
        "WardGuide notes that copper issues are relatively localized and soil type matters substantially."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Zinc, Iron, Manganese and Copper",
            anchor="soil-test-methods-zinc-iron-manganese-and-copper",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-zinc-iron-manganese-and-copper",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 27",
            table_title="Copper Fertilizer Recommendations",
            page_hint=46,
            source_url=f"{WARD_GUIDE_HTML}#table-27-copper-fertilizer-recommendations",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 62",
            table_title="Zinc, Iron, Manganese and Copper Availability Ratings for Various Crops",
            page_hint=102,
            source_url=f"{WARD_GUIDE_HTML}#table-62-zinc-iron-manganese-and-copper-availability-ratings-for-various-crops",
        ),
    ),
)

WATER_STABLE_AGGREGATES_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Water-stable aggregates reflect how well soil particles remain bound together when wetted."
    ),
    detail=(
        "WardGuide connects aggregate stability to organic matter, microbial activity, and soil structure. "
        "It notes that microbial products help bind particles together and that management practices that "
        "build organic matter can help improve structural stability and water relations."
    ),
    interpretation=(
        "Higher water-stable aggregation generally suggests stronger soil structure, better resistance to "
        "slaking and crusting, and improved conditions for infiltration, aeration, and root growth."
    ),
    caveat=(
        "Interpret aggregate stability in context. Texture, organic matter, residue management, tillage, "
        "and biological activity all influence the measurement, so it is most useful for comparing treatments "
        "or tracking change over time."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Texture",
            anchor="soil-texture",
            page_hint=120,
            source_url=f"{WARD_GUIDE_HTML}#soil-texture",
        ),
    ),
)

BASE_SATURATION_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Percent base saturation describes the share of exchange sites occupied by base cations such as potassium, calcium, magnesium, and sodium."
    ),
    detail=(
        "WardGuide explains that percent base saturation reflects how much of the CEC is occupied by exchangeable bases rather than hydrogen "
        "and aluminum, and that it is linked to soil acidity and cation balance."
    ),
    interpretation=(
        "Interpret base saturation in relation to pH, CEC, region, and broader cation context rather than as a rigid universal target."
    ),
    caveat=(
        "WardGuide notes that ratios and saturation values can vary widely without necessarily causing yield loss, so interpretation should remain contextual."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Percent Base Saturation",
            anchor="percent-base-saturation",
            page_hint=121,
            source_url=f"{WARD_GUIDE_HTML}#percent-base-saturation",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soil Test Methods: Calcium and Magnesium",
            anchor="soil-test-methods-calcium-and-magnesium",
            page_hint=103,
            source_url=f"{WARD_GUIDE_HTML}#soil-test-methods-calcium-and-magnesium",
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
            section_title="Biological Indicators Table",
            anchor="table-1-biological",
            page_hint=1,
            source_url=f"{SHA_GUIDE_HTML}#table-1-biological",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 66",
            table_title="Soil Respiration Ranking Table",
            page_hint=113,
            source_url=f"{WARD_GUIDE_HTML}#table-66-soil-respiration-ranking-table",
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
            anchor="microbially-active-carbon-mac",
            page_hint=3,
            source_url=f"{SHA_GUIDE_HTML}#microbially-active-carbon-mac",
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
            table_title="Organic C:N Ratio Ranking Table",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#table-68-organic-cn-ration-ranking-table",
        ),
    ),
    thresholds=None,
)

# ---------------------------------------------------------------------
# NIR / forage references
# ---------------------------------------------------------------------

NIRS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Near-Infrared Spectroscopy (NIRS) is used to estimate forage constituents quickly and efficiently."
    ),
    detail=(
        "WardGuide describes NIRS as a rapid secondary analytical method based on reflectance. "
        "It is commonly used to estimate crude protein, soluble protein, non-protein nitrogen (npn), "
        "fiber fractions including crude fiber, acid detergent fiber (adf), and neutral detergent fiber (ndf), "
        "as well as energy-related values such as total digestible nutrients (tdn), digestible energy, "
        "relative feed value (rfv), and relative forage quality (rfq). NIRS allows fast turnaround and is "
        "widely used for routine forage evaluation when appropriate calibrations are available."
    ),
    interpretation=(
        "NIRS is most useful as a rapid screening and evaluation tool for forage quality. It performs best "
        "when the sample type matches the calibration set used by the laboratory. Fiber fractions, protein "
        "measures, and derived indices like rfv and rfq are commonly interpreted together to understand "
        "intake potential, digestibility, and overall feed value."
    ),
    caveat=(
        "WardGuide emphasizes that NIRS is a secondary method calibrated against wet chemistry. Accuracy depends "
        "on calibration quality and sample fit. Some analytes may require wet chemistry confirmation for precise "
        "ration formulation or diagnostic work."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

CRUDE_PROTEIN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Crude protein estimates the total protein-related nitrogen content of the forage."
    ),
    detail=(
        "WardGuide explains that crude protein is not actually a direct measurement of true protein, "
        "but of nitrogen. Total nitrogen in the sample is measured and then converted to crude protein "
        "with a factor appropriate to the feed type. In forage evaluation, crude protein is one of the "
        "core nutrient tests used to characterize feed value."
    ),
    interpretation=(
        "Use crude protein as a basic indicator of forage protein supply, while remembering that it is "
        "a nitrogen-based estimate rather than a direct measure of true protein composition."
    ),
    caveat=(
        "WardGuide notes that crude protein alone does not fully describe protein nutrition. In some "
        "feeding contexts, especially ruminant systems, soluble protein, undegradable protein, heat "
        "damage, and other protein fractions may also matter. When reported from NIRS, interpretation "
        "should also consider calibration and method limits."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Protein",
            anchor="protein",
            page_hint=19,
            source_url=f"{WARD_GUIDE_HTML}#protein",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

ACID_DETERGENT_FIBER_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Acid Detergent Fiber (ADF) is the least digestible portion of a feed and indicates forage indigestibility."
    ),
    detail=(
        "WardGuide describes ADF as the fraction containing cellulose, lignin, and pectin, but not "
        "hemicellulose. It is used to help predict the energy and digestibility of a feed. As ADF "
        "increases, digestibility and energy value generally decrease; as ADF decreases, digestibility "
        "and energy value generally increase."
    ),
    interpretation=(
        "ADF is commonly used as a practical indicator of forage quality because it is inversely related "
        "to digestibility and energy value."
    ),
    caveat=(
        "WardGuide notes that ADF is also used in calculating relative feed value and relative forage "
        "quality, so it should be interpreted as part of the broader forage-quality picture rather than "
        "in isolation. When estimated by NIRS, ADF should also be viewed in light of method calibration."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Acid Detergent Fiber (ADF)",
            anchor="acid-detergent-fiber-adf",
            page_hint=20,
            source_url=f"{WARD_GUIDE_HTML}#acid-detergent-fiber-adf",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NEUTRAL_DETERGENT_FIBER_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Neutral Detergent Fiber (NDF) estimates the total cell wall fraction and is commonly used as an indicator of intake potential."
    ),
    detail=(
        "WardGuide treats NDF as a core fiber measurement used in forage evaluation. NDF represents the "
        "plant cell wall fraction and is widely used because it helps describe bulkiness and expected intake "
        "limitations of forage."
    ),
    interpretation=(
        "Higher NDF values generally indicate a bulkier forage that may limit intake, while lower NDF values "
        "generally indicate greater intake potential."
    ),
    caveat=(
        "NDF is valuable, but it does not by itself describe digestibility or total feed value. It is most "
        "useful when interpreted together with ADF, energy estimates, and other forage-quality measures. "
        "When estimated by NIRS, interpretation should also consider method fit and calibration."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Neutral Detergent Fiber (NDF)",
            anchor="neutral-detergent-fiber-ndf",
            page_hint=21,
            source_url=f"{WARD_GUIDE_HTML}#neutral-detergent-fiber-ndf",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

TOTAL_DIGESTIBLE_NUTRIENTS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Total Digestible Nutrients (TDN) is a traditional energy-related measure used to estimate feed value."
    ),
    detail=(
        "WardGuide describes TDN as the sum of digestible crude protein, digestible nitrogen-free extract, "
        "digestible crude fiber, and digestible ether extract, with fat contributing more energy than the other "
        "fractions. TDN is used to predict energy values of feed for beef cattle, dairy cattle, sheep, and goats."
    ),
    interpretation=(
        "TDN is a practical summary estimate of forage energy value and is often used in ration evaluation for ruminants."
    ),
    caveat=(
        "WardGuide notes that TDN can be estimated in more than one way. Values derived from full proximate analysis "
        "are generally more informative than simpler estimates based only on ADF or crude fiber. When reported from "
        "NIRS-based forage analysis, interpretation should include the underlying method context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Total Digestible Nutrients (TDN)",
            anchor="total-digestible-nutrients-tdn",
            page_hint=24,
            source_url=f"{WARD_GUIDE_HTML}#total-digestible-nutrients-tdn",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

RFV_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Relative Feed Value (RFV) is an index used to compare forage quality."
    ),
    detail=(
        "WardGuide presents RFV as a forage quality index derived from fiber-based estimates of intake "
        "and digestibility. It is commonly used as a quick comparative measure for hay and forage quality."
    ),
    interpretation=(
        "Higher RFV generally indicates higher quality forage in comparative terms."
    ),
    caveat=(
        "RFV is a comparative index, not a complete nutritional description. It is useful for ranking forages, "
        "but it should not replace full ration interpretation. When RFV is derived from NIRS-estimated forage traits, "
        "the underlying method context also matters."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Relative Feed Value (RFV)",
            anchor="relative-feed-value-rfv",
            page_hint=27,
            source_url=f"{WARD_GUIDE_HTML}#relative-feed-value-rfv",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 7",
            table_title="Relative Forage Quality Suggested for Different Cattle Types",
            page_hint=28,
            source_url=f"{WARD_GUIDE_HTML}#table-7-relative-forage-quality-suggested-for-different-cattle-types",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

RFQ_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Relative Forage Quality (RFQ) is a forage quality index intended to improve on RFV by incorporating fiber digestibility."
    ),
    detail=(
        "WardGuide includes RFQ as a comparative forage-quality measure that builds on the older RFV concept. "
        "RFQ is intended to provide a more complete estimate of forage value by including digestibility-related information."
    ),
    interpretation=(
        "Higher RFQ generally indicates better forage quality in comparative terms, especially when digestibility differences matter."
    ),
    caveat=(
        "Like RFV, RFQ is still an index rather than a full nutritional profile. It is best used for comparison and screening, "
        "with full ration formulation based on the broader nutrient context. When derived from NIRS-estimated values, "
        "interpretation should include the NIRS method context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Relative Forage Quality (RFQ)",
            anchor="relative-forage-quality-rfq",
            page_hint=28,
            source_url=f"{WARD_GUIDE_HTML}#relative-forage-quality-rfq",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            table_number="Table 7",
            table_title="Relative Forage Quality Suggested for Different Cattle Types",
            page_hint=28,
            source_url=f"{WARD_GUIDE_HTML}#table-7-relative-forage-quality-suggested-for-different-cattle-types",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

SOLUBLE_PROTEIN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Soluble protein estimates the protein fraction that is readily degraded and available to rumen microbes."
    ),
    detail=(
        "WardGuide explains that soluble protein is particularly relevant in ruminant feeding because it reflects "
        "the protein fraction that will feed rumen microbes. It is part of the broader partitioning of crude protein "
        "into nutritionally meaningful fractions."
    ),
    interpretation=(
        "Soluble protein is most useful in ration balancing for ruminants, especially when considering rumen degradable protein."
    ),
    caveat=(
        "This measure is most meaningful in ruminant nutrition contexts and should be interpreted alongside other protein fractions. "
        "If reported from an NIRS workflow, method limitations should also be considered."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Soluble Protein",
            anchor="soluble-protein",
            page_hint=19,
            source_url=f"{WARD_GUIDE_HTML}#soluble-protein",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NPN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Non-Protein Nitrogen (NPN) represents nitrogen that is not present as true protein."
    ),
    detail=(
        "WardGuide describes non-protein nitrogen as a nitrogen source that can be utilized by rumen microbes, "
        "with urea being a common example. In ruminant systems, this nitrogen can contribute to microbial protein "
        "production when the diet is balanced appropriately."
    ),
    interpretation=(
        "NPN is mainly relevant in ruminant feeding systems where microbial utilization of nitrogen is part of ration design."
    ),
    caveat=(
        "NPN should not be interpreted the same way as true protein. Its usefulness depends strongly on animal type and diet context. "
        "Where NIRS is used in the reporting workflow, interpretation should also account for method limitations."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Non-Protein Nitrogen (NPN)",
            anchor="non-protein-nitrogen-npn",
            page_hint=19,
            source_url=f"{WARD_GUIDE_HTML}#non-protein-nitrogen-npn",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

ADIP_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Damaged Protein or Acid Detergent Insoluble Protein (ADIP) estimates protein tied up in less available, heat-damaged forms."
    ),
    detail=(
        "WardGuide explains that heating under poor storage conditions can cause proteins to bind to lignin and other feed components, "
        "reducing their nutritional availability. ADIP is used to help identify heat-damaged protein and determine whether crude protein "
        "should be adjusted for ration work."
    ),
    interpretation=(
        "ADIP is especially useful when evaluating stored forages or feeds where heat damage may have reduced usable protein value."
    ),
    caveat=(
        "A crude protein value may overstate nutritionally available protein if ADIP is elevated, so these values should be interpreted together. "
        "Because this kind of measure may require stronger analytical specificity, any NIRS-based context should be interpreted cautiously."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Damaged Protein or Acid Detergent Insoluble Protein (ADIP)",
            anchor="damaged-protein-or-acid-detergent-insoluble-protein-adip",
            page_hint=19,
            source_url=f"{WARD_GUIDE_HTML}#damaged-protein-or-acid-detergent-insoluble-protein-adip",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NFC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Non-fiber carbohydrates (NFC) estimate the rapidly available carbohydrate fraction not contained in the fiber pool."
    ),
    detail=(
        "In forage analysis, NFC is used as a summary carbohydrate measure complementary to fiber fractions. It helps describe "
        "the more readily available energy portion of the feed after fiber, protein, fat, and ash are considered."
    ),
    interpretation=(
        "Interpret NFC together with fiber, starch, water-soluble carbohydrates, and energy measures rather than as an isolated value."
    ),
    caveat=(
        "When reported through NIRS, NFC should be interpreted as a modeled forage-quality estimate whose reliability depends on calibration quality."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

STARCH_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Starch is a rapidly available carbohydrate and an important energy indicator in feed evaluation."
    ),
    detail=(
        "WardGuide describes total starch as a measurement of starch in feed and notes that starch is "
        "a rapidly available carbohydrate. High-starch feeds are generally high-energy feeds such as "
        "cereal grains, corn, and corn silage."
    ),
    interpretation=(
        "Interpret starch in relation to NFC, fiber, digestibility, total energy profile, and animal "
        "feeding goals."
    ),
    caveat=(
        "WardGuide notes that high-starch diets can increase bloat risk in feedlot steers. When starch "
        "is reported from NIRS, it remains a modeled estimate and should be interpreted with method "
        "context in mind."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

WSC_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Water-soluble carbohydrates (WSC) estimate the readily soluble sugar fraction of the forage."
    ),
    detail=(
        "WSC helps describe the quickly available carbohydrate component that can influence fermentation, palatability, and ration behavior."
    ),
    interpretation=(
        "Interpret WSC together with starch, NFC, fiber fractions, and the intended feeding context."
    ),
    caveat=(
        "When estimated by NIRS, WSC should be read as a method-dependent forage-quality estimate rather than as a fully direct measurement."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

FRUCTAN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Fructans are storage carbohydrates that contribute to the soluble carbohydrate profile of some forages."
    ),
    detail=(
        "Fructans are part of the rapidly available carbohydrate pool and can be important in understanding forage sugar composition."
    ),
    interpretation=(
        "Interpret fructans together with WSC, starch, and overall forage carbohydrate composition."
    ),
    caveat=(
        "When reported from NIRS, fructan values are calibration-dependent estimates and should be interpreted with method context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NEL_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Net energy for lactation (NEL) is an energy estimate used in ration planning for lactating animals."
    ),
    detail=(
        "WardGuide places NEL within the net energy framework and defines NEl as net energy of lactation "
        "calculated from ADF."
    ),
    interpretation=(
        "Interpret NEL as part of the broader energy profile, together with digestibility, TDN, fiber fractions, and animal class."
    ),
    caveat=(
        "When derived through NIRS-based forage analysis, NEL should be interpreted as a modeled energy estimate."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Net Energy (NE)",
            anchor="net-energy-ne",
            page_hint=25,
            source_url=f"{WARD_GUIDE_HTML}#net-energy-ne",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NEM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Net energy for maintenance (NEM) estimates the energy value of feed for maintenance requirements."
    ),
    detail=(
        "WardGuide places NEM within the net energy framework and defines NEm as the net energy value "
        "of feeds for maintenance calculated from TDN."
    ),
    interpretation=(
        "Interpret NEM with other energy and digestibility measures rather than by itself."
    ),
    caveat=(
        "When derived from NIRS-based forage analysis, NEM should be treated as a modeled estimate dependent "
        "on the calibration workflow."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Net Energy (NE)",
            anchor="net-energy-ne",
            page_hint=25,
            source_url=f"{WARD_GUIDE_HTML}#net-energy-ne",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NEG_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Net energy for gain (NEG) estimates the feed energy available for weight gain."
    ),
    detail=(
        "WardGuide places NEG within the net energy framework and defines NEg as the net energy value "
        "of feeds for the deposition of body tissue, growth, or gain calculated from TDN."
    ),
    interpretation=(
        "Interpret NEG in the context of the broader energy profile, animal class, and feeding objective."
    ),
    caveat=(
        "When reported through NIRS, NEG should be interpreted as a derived estimate rather than a direct chemical measurement."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Net Energy (NE)",
            anchor="net-energy-ne",
            page_hint=25,
            source_url=f"{WARD_GUIDE_HTML}#net-energy-ne",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

ASH_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Ash represents the total mineral residue remaining after combustion of the feed."
    ),
    detail=(
        "Ash is used as a broad measure of total mineral content and can help interpret whether a feed sample contains more or less total mineral residue."
    ),
    interpretation=(
        "Interpret ash with the rest of the nutrient profile, especially when comparing feeds or looking for unusual mineral concentration patterns."
    ),
    caveat=(
        "Ash is a broad summary measurement and does not identify which specific minerals are responsible for the total residue. "
        "When estimated through NIRS, it remains method-dependent."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

NDFD48_REFERENCE = VariableReferenceBundle(
    short_note=(
        "NDF digestibility at 48 hours estimates how much of the neutral detergent fiber fraction is digested during a 48-hour in vitro incubation."
    ),
    detail=(
        "University of Wisconsin forage guidance explains that in vitro NDF digestibility is measured by incubating forage in buffer and live rumen fluid under anaerobic conditions. "
        "The 48-hour value is typically higher than the 30-hour value because the incubation is longer, and 48-hour assays are often considered more repeatable in the laboratory."
    ),
    interpretation=(
        "Interpret 48-hour NDF digestibility together with NDF, ADF, RFQ, and broader energy measures. Higher NDF digestibility is associated with better forage utilization and can influence intake potential and energy prediction."
    ),
    caveat=(
        "Thirty-hour and 48-hour NDF digestibility values are related but not directly interchangeable. Differences vary by forage type, so incubation time and laboratory method should be noted when comparing values."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Neutral Detergent Fiber (NDF)",
            anchor="neutral-detergent-fiber-ndf",
            page_hint=21,
            source_url=f"{WARD_GUIDE_HTML}#neutral-detergent-fiber-ndf",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

IVTDMD48_REFERENCE = VariableReferenceBundle(
    short_note=(
        "In vitro true digestibility at 48 hours (IVTDMD) estimates the total digestible portion "
        "of forage using a standardized laboratory simulation of rumen digestion."
    ),
    detail=(
        "IVTDMD is measured by incubating forage samples in rumen fluid for approximately 48 hours "
        "to simulate digestion in a ruminant animal. It represents the proportion of total dry matter "
        "that is digestible.\n\n"
        "This metric is closely related to NDF digestibility (NDFD). Both values are derived from the "
        "same laboratory procedure: NDFD expresses digestibility as a percentage of fiber (NDF), while "
        "IVTDMD expresses digestibility as a percentage of total dry matter.\n\n"
        "Higher IVTDMD values indicate greater forage digestibility, which is generally associated with "
        "higher energy availability, improved intake, and better animal performance."
    ),
    interpretation=(
        "Interpret IVTDMD together with NDF, NDF digestibility (NDFD), and energy-related metrics such "
        "as TDN or RFQ. High IVTDMD combined with moderate or low fiber content typically indicates "
        "high-quality forage."
    ),
    caveat=(
        "IVTDMD and NDFD are derived from the same laboratory digestion process but represent different "
        "expressions of digestibility (total dry matter vs. fiber fraction). These values should be "
        "interpreted together rather than interchangeably."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
        ReferenceInfo(
            guide_key="uw_extension",
            guide_label="University of Wisconsin Extension",
            section_title="In Vitro NDF Digestibility of Forages: The 30 vs. 48 Hour Debate",
            anchor=None,
            page_hint=None,
            source_url="https://fyi.extension.wisc.edu/forage/in-vitro-ndf-digestibility-of-forages-the-30-vs-48-hour-debate/",
        ),
    ),
)

FAT_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Crude fat estimates the lipid fraction of the feed and contributes to its energy profile."
    ),
    detail=(
        "Fat is one of the standard compositional measures used in feed and forage evaluation and contributes disproportionately to energy value."
    ),
    interpretation=(
        "Interpret fat as part of the broader nutrient and energy profile rather than as a stand-alone number."
    ),
    caveat=(
        "When reported through NIRS, crude fat should be interpreted as a method-dependent estimate."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

LIGNIN_REFERENCE = VariableReferenceBundle(
    short_note=(
        "Lignin is a structural component associated with reduced fiber digestibility."
    ),
    detail=(
        "Lignin contributes to plant structural rigidity and is commonly interpreted as part of the indigestible or less digestible fiber framework."
    ),
    interpretation=(
        "Interpret lignin together with ADF, NDF, digestibility measures, and forage maturity."
    ),
    caveat=(
        "Because lignin is closely tied to digestibility and maturity, it is best interpreted comparatively and with awareness of method context."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="Near-Infrared Spectroscopy (NIRS)",
            anchor="near-infrared-spectroscopy-nirs",
            page_hint=29,
            source_url=f"{WARD_GUIDE_HTML}#near-infrared-spectroscopy-nirs",
        ),
    ),
)

# ---------------------------------------------------------------------
# Soil biomass / PLFA references
# ---------------------------------------------------------------------

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
            guide_key="ward_report",
            guide_label=WARD_BIOLOGICAL_REPORT_LABEL,
            section_title="Total Biomass and Diversity Ratings",
            anchor="diversity-index-ratings",
            page_hint=2,
            source_url=f"{WARD_BIOLOGICAL_REPORT_HTML}#diversity-index-ratings",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ng/g",
        method_note=(
            f"Ward PLFA total biomass rating scale from the {WARD_BIOLOGICAL_REPORT_DATE} "
            "Ward Biological Report. These categories are method-specific to Ward's PLFA workflow."
        ),
        bands=(
            InterpretationBand(label="Very Poor", min_value=0, max_value=500),
            InterpretationBand(label="Poor", min_value=500, max_value=1000),
            InterpretationBand(label="Slightly Below Average", min_value=1000, max_value=1500),
            InterpretationBand(label="Average", min_value=1500, max_value=2500),
            InterpretationBand(label="Slightly Above Average", min_value=2500, max_value=3000),
            InterpretationBand(label="Good", min_value=3000, max_value=3500),
            InterpretationBand(label="Very Good", min_value=3500, max_value=4000),
            InterpretationBand(label="Excellent", min_value=4000, max_value=None),
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

DIVERSITY_INDEX_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The diversity index summarizes how broadly distributed the living PLFA signal is across microbial groups."
    ),
    detail=(
        "In Ward's PLFA reporting, the diversity index is used alongside total biomass to describe "
        "the breadth of the living microbial community. A higher index generally reflects a more "
        "evenly represented or more diverse microbial community profile within Ward's analytical framework."
    ),
    interpretation=(
        "Higher diversity index values are generally associated with broader microbial representation. "
        "Ward's biological report provides interpretation bands from Very Poor through Excellent."
    ),
    caveat=(
        "These thresholds are specific to Ward's PLFA extraction and analytical method and should be "
        "used comparatively within that reporting framework."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_report",
            guide_label=WARD_BIOLOGICAL_REPORT_LABEL,
            section_title="Total Biomass and Diversity Ratings",
            anchor="diversity-index-ratings",
            page_hint=2,
            source_url=f"{WARD_BIOLOGICAL_REPORT_HTML}#diversity-index-ratings",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="index",
        method_note=(
            f"Ward PLFA diversity rating scale from the {WARD_BIOLOGICAL_REPORT_DATE} "
            "Ward Biological Report. These categories are method-specific to Ward's PLFA workflow."
        ),
        bands=(
            InterpretationBand(label="Very Poor", min_value=0, max_value=1.0),
            InterpretationBand(label="Poor", min_value=1.0, max_value=1.1),
            InterpretationBand(label="Slightly Below Average", min_value=1.1, max_value=1.2),
            InterpretationBand(label="Average", min_value=1.2, max_value=1.3),
            InterpretationBand(label="Slightly Above Average", min_value=1.3, max_value=1.4),
            InterpretationBand(label="Good", min_value=1.4, max_value=1.5),
            InterpretationBand(label="Very Good", min_value=1.5, max_value=1.6),
            InterpretationBand(label="Excellent", min_value=1.6, max_value=None),
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
        ReferenceInfo(
            guide_key="ward_report",
            guide_label=WARD_BIOLOGICAL_REPORT_LABEL,
            section_title="Fungi:Bacteria Ratings",
            anchor="fungi-bacteria-ratings",
            page_hint=8,
            source_url=f"{WARD_BIOLOGICAL_REPORT_HTML}#fungi-bacteria-ratings",
        ),
    ),
    thresholds=None,
)

PREDATOR_PREY_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The predator-to-prey ratio compares predator biomass, typically protozoa, to prey biomass, typically bacteria."
    ),
    detail=(
        "Ward's PLFA biological report includes predator:prey as a community-structure indicator. "
        "It is used to characterize the balance between microbial grazers and the bacterial prey base."
    ),
    interpretation=(
        "Higher predator:prey values indicate greater predator presence relative to prey. "
        "Ward's report provides method-specific interpretation bands from Very Poor through Excellent."
    ),
    caveat=(
        "This ratio should be interpreted comparatively within Ward's PLFA framework and not as a universal "
        "stand-alone indicator of soil function."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_report",
            guide_label=WARD_BIOLOGICAL_REPORT_LABEL,
            section_title="Predator:Prey Ratings",
            anchor="predator-prey-ratings",
            page_hint=4,
            source_url=f"{WARD_BIOLOGICAL_REPORT_HTML}#predator-prey-ratings",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ratio",
        method_note=(
            f"Ward PLFA predator:prey rating scale from the {WARD_BIOLOGICAL_REPORT_DATE} "
            "Ward Biological Report. These categories are method-specific to Ward's PLFA workflow."
        ),
        bands=(
            InterpretationBand(label="Very Poor", min_value=0, max_value=0.05),
            InterpretationBand(label="Poor", min_value=0.05, max_value=0.10),
            InterpretationBand(label="Slightly Below Average", min_value=0.10, max_value=0.15),
            InterpretationBand(label="Average", min_value=0.15, max_value=0.20),
            InterpretationBand(label="Slightly Above Average", min_value=0.20, max_value=0.25),
            InterpretationBand(label="Good", min_value=0.25, max_value=0.30),
            InterpretationBand(label="Very Good", min_value=0.30, max_value=0.35),
            InterpretationBand(label="Excellent", min_value=0.35, max_value=None),
        ),
    ),
)

GRAM_POS_GRAM_REFERENCE = VariableReferenceBundle(
    short_note=(
        "The Gram(+):Gram(-) ratio summarizes the relative dominance of Gram-positive and Gram-negative bacteria."
    ),
    detail=(
        "Ward's PLFA biological report uses the Gram(+):Gram(-) ratio to characterize the structure of the bacterial "
        "community, ranging from Gram(-)-dominated to strongly Gram(+)-dominated."
    ),
    interpretation=(
        "Values near the middle of the scale reflect a more balanced bacterial community, while low or high values "
        "indicate stronger dominance by one group."
    ),
    caveat=(
        "These thresholds are specific to Ward's PLFA method and should be interpreted comparatively within that framework."
    ),
    references=(
        ReferenceInfo(
            guide_key="ward_report",
            guide_label=WARD_BIOLOGICAL_REPORT_LABEL,
            section_title="Gram (+):Gram (-) Ratings",
            anchor="gram-pos-gram-neg-ratings",
            page_hint=6,
            source_url=f"{WARD_BIOLOGICAL_REPORT_HTML}#gram-pos-gram-neg-ratings",
        ),
        ReferenceInfo(
            guide_key="ward_guide",
            guide_label="WardGuide",
            section_title="PLFA",
            anchor="plfa",
            page_hint=114,
            source_url=f"{WARD_GUIDE_HTML}#plfa",
        ),
    ),
    thresholds=InterpretationInfo(
        unit_label="ratio",
        method_note=(
            f"Ward PLFA Gram(+):Gram(-) rating scale from the {WARD_BIOLOGICAL_REPORT_DATE} "
            "Ward Biological Report. These categories are method-specific to Ward's PLFA workflow."
        ),
        bands=(
            InterpretationBand(label="Gram (-) Dominated", min_value=0, max_value=0.5),
            InterpretationBand(label="Slightly Gram (-) Dominated", min_value=0.5, max_value=1.0),
            InterpretationBand(label="Balanced Bacterial Community", min_value=1.0, max_value=2.0),
            InterpretationBand(label="Slightly Gram(+) Dominated", min_value=2.0, max_value=3.0),
            InterpretationBand(label="Gram(+) Dominated", min_value=3.0, max_value=4.0),
            InterpretationBand(label="Very Gram(+) Dominated", min_value=4.0, max_value=None),
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

PLFA_FUNCTIONAL_GROUP_REFERENCE = VariableReferenceBundle(
    short_note=(
        "These PLFA functional-group biomass values estimate the relative size of specific living microbial subgroups within the soil community."
    ),
    detail=(
        "WardGuide explains that PLFA analysis uses membrane lipids from living organisms to estimate both total biomass "
        "and biomass of selected microbial groups. Functional-group values such as actinobacteria, rhizobia, saprophytes, "
        "and undifferentiated biomass are best understood as subgroup components of the broader PLFA-measured living community."
    ),
    interpretation=(
        "Interpret these subgroup biomass values comparatively across treatments, dates, and management systems. They are most "
        "useful for detecting shifts in community composition rather than as stand-alone targets."
    ),
    caveat=(
        "Ward's uploaded materials provide stronger support for general PLFA group interpretation than for highly specific threshold "
        "cutoffs for each subgroup. Use these variables as comparative indicators within the same analytical framework."
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
    thresholds=None,
)

PLFA_STRESS_REFERENCE = VariableReferenceBundle(
    short_note=(
        "These PLFA stress and membrane-structure ratios are comparative indicators of shifts in microbial physiology and community condition."
    ),
    detail=(
        "Ward's PLFA framework includes fatty-acid ratios that are commonly used as comparative indicators of stress response, "
        "membrane composition, and broad changes in microbial community condition. Ratios such as precursor-to-cyclopropyl forms "
        "and saturated-to-unsaturated relationships are generally interpreted as relative indicators rather than direct measures of a single process."
    ),
    interpretation=(
        "Interpret these ratios comparatively across treatments and sampling dates, and alongside total biomass, community structure, "
        "and site conditions such as moisture, residue inputs, and management."
    ),
    caveat=(
        "These are method-specific comparative ratios. Ward's uploaded materials support PLFA-based community interpretation, but they do "
        "not provide strong universal threshold categories for these specific stress indicators. Avoid overinterpreting small differences."
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
    thresholds=None,
)

CA_NIR_REFERENCE = combine_reference_bundles(CALCIUM_REFERENCE, NIRS_REFERENCE)
P_NIR_REFERENCE = combine_reference_bundles(PHOSPHORUS_REFERENCE, NIRS_REFERENCE)
K_NIR_REFERENCE = combine_reference_bundles(POTASSIUM_REFERENCE, NIRS_REFERENCE)
MG_NIR_REFERENCE = combine_reference_bundles(MAGNESIUM_REFERENCE, NIRS_REFERENCE)

# ---------------------------------------------------------------------
# Master lookup
# ---------------------------------------------------------------------

LAB_REFERENCES = {
    # -----------------------------------------------------------------
    # Soil chemistry: generic keys
    # -----------------------------------------------------------------
    "phosphorus": PHOSPHORUS_REFERENCE,
    "potassium": POTASSIUM_REFERENCE,
    "organic_matter": ORGANIC_MATTER_REFERENCE,
    "cec": CEC_REFERENCE,
    "ph": PH_REFERENCE,
    "buffer_ph": BUFFER_PH_REFERENCE,
    "salinity": SALINITY_REFERENCE,
    "excess_lime": EXCESS_LIME_REFERENCE,
    "nitrate": NITRATE_REFERENCE,
    "sulfur": SULFUR_REFERENCE,
    "calcium": CALCIUM_REFERENCE,
    "magnesium": MAGNESIUM_REFERENCE,
    "sodium": SODIUM_REFERENCE,
    "zinc": ZINC_REFERENCE,
    "iron": IRON_REFERENCE,
    "manganese": MANGANESE_REFERENCE,
    "copper": COPPER_REFERENCE,
    "base_saturation": BASE_SATURATION_REFERENCE,
    "water_stable_aggregates": WATER_STABLE_AGGREGATES_REFERENCE,
    "water_stable_aggregates_mod": WATER_STABLE_AGGREGATES_REFERENCE,

    # -----------------------------------------------------------------
    # Soil chemistry: exact table variable keys
    # -----------------------------------------------------------------
    "soil_ph_1_1": PH_REFERENCE,
    "ec_1_1": SALINITY_REFERENCE,
    "organic_matter_loi_pct": ORGANIC_MATTER_REFERENCE,
    "olsen_p_ppm_p": PHOSPHORUS_REFERENCE,
    "potassium_ppm_k": POTASSIUM_REFERENCE,
    "sulfate_s_ppm_s": SULFUR_REFERENCE,
    "nitrate_n_ppm": NITRATE_REFERENCE,
    "zinc_ppm_zn": ZINC_REFERENCE,
    "iron_ppm_fe": IRON_REFERENCE,
    "manganese_ppm_mn": MANGANESE_REFERENCE,
    "copper_ppm_cu": COPPER_REFERENCE,
    "calcium_ppm_ca": CALCIUM_REFERENCE,
    "magnesium_ppm_mg": MAGNESIUM_REFERENCE,
    "sodium_ppm_na": SODIUM_REFERENCE,
    "cec_meq_100g": CEC_REFERENCE,
    "cec_sum_of_cations_me_100g": CEC_REFERENCE,
    "pcth_sat": BASE_SATURATION_REFERENCE,
    "pctk_sat": BASE_SATURATION_REFERENCE,
    "pctca_sat": BASE_SATURATION_REFERENCE,
    "pctmg_sat": BASE_SATURATION_REFERENCE,
    "pctna_sat": BASE_SATURATION_REFERENCE,

    # -----------------------------------------------------------------
    # Soil chemistry / recommendations section
    # -----------------------------------------------------------------
    "yg_1": None,
    "nitrogen_rec": NITRATE_REFERENCE,
    "p2o5_rec": PHOSPHORUS_REFERENCE,
    "k2o_rec": POTASSIUM_REFERENCE,
    "sulfur_rec": SULFUR_REFERENCE,
    "zinc_rec": ZINC_REFERENCE,
    "magnesium_rec": MAGNESIUM_REFERENCE,
    "iron_rec": IRON_REFERENCE,
    "manganese_rec": MANGANESE_REFERENCE,
    "copper_rec": COPPER_REFERENCE,

    # -----------------------------------------------------------------
    # Soil chemistry / soil health & water extract
    # -----------------------------------------------------------------
    "h2o_no3_n": NITRATE_REFERENCE,
    "h2o_nh4_n": None,
    "total_n_h2o_ppm_n": None,
    "organic_c_h2o_ppm": WEOC_REFERENCE,
    "organic_n_h2o_ppm": WEON_REFERENCE,
    "organic_c_n_h2o": ORGANIC_CN_REFERENCE,
    "co2_soil_respiration": SOIL_RESPIRATION_REFERENCE,
    "microbially_active_carbon_pctma": MAC_REFERENCE,
    "organic_nitrogen_release_ppm_n": None,
    "organic_nitrogen_reserve_ppm_n": None,

    # -----------------------------------------------------------------
    # Soil biology / soil health
    # -----------------------------------------------------------------
    "soil_respiration": SOIL_RESPIRATION_REFERENCE,
    "weoc": WEOC_REFERENCE,
    "weon": WEON_REFERENCE,
    "mac": MAC_REFERENCE,
    "soil_health_score": SOIL_HEALTH_SCORE_REFERENCE,
    "organic_cn": ORGANIC_CN_REFERENCE,

    # -----------------------------------------------------------------
    # Soil biomass / PLFA generic bundles
    # -----------------------------------------------------------------
    "plfa_functional_group": PLFA_FUNCTIONAL_GROUP_REFERENCE,
    "plfa_stress": PLFA_STRESS_REFERENCE,

    # -----------------------------------------------------------------
    # Soil biomass / PLFA main keys
    # -----------------------------------------------------------------
    "total_biomass": TOTAL_BIOMASS_REFERENCE,
    "bacteria_biomass": BACTERIA_BIOMASS_REFERENCE,
    "fungi_biomass": FUNGI_BIOMASS_REFERENCE,
    "diversity_index": DIVERSITY_INDEX_REFERENCE,
    "fungi_bacteria": FUNGI_BACTERIA_REFERENCE,
    "predator_prey": PREDATOR_PREY_REFERENCE,
    "gram_pos_gram": GRAM_POS_GRAM_REFERENCE,
    "mycorrhizae_biomass": MYCORRHIZAE_BIOMASS_REFERENCE,

    # -----------------------------------------------------------------
    # Soil biomass / PLFA subgroup keys
    # -----------------------------------------------------------------
    "actinobacteria_biomass": PLFA_FUNCTIONAL_GROUP_REFERENCE,
    "rhizobia_biomass": PLFA_FUNCTIONAL_GROUP_REFERENCE,
    "saprophytes_biomass": PLFA_FUNCTIONAL_GROUP_REFERENCE,
    "undifferentiated_biomass": PLFA_FUNCTIONAL_GROUP_REFERENCE,

    # -----------------------------------------------------------------
    # Soil biomass / PLFA stress keys
    # -----------------------------------------------------------------
    "pre_16_1w7c_cy17_0": PLFA_STRESS_REFERENCE,
    "pre_18_1w7c_cy19_0": PLFA_STRESS_REFERENCE,
    "sat_unsat": PLFA_STRESS_REFERENCE,
    "mono_poly": PLFA_STRESS_REFERENCE,

    # -----------------------------------------------------------------
    # NIR / forage primary keys
    # -----------------------------------------------------------------
    "nirs": NIRS_REFERENCE,
    "crude_protein_pct_db": CRUDE_PROTEIN_REFERENCE,
    "adf_pct_db": ACID_DETERGENT_FIBER_REFERENCE,
    "ndf_pct_db": NEUTRAL_DETERGENT_FIBER_REFERENCE,
    "tdn_pct_db": TOTAL_DIGESTIBLE_NUTRIENTS_REFERENCE,
    "rfv": RFV_REFERENCE,
    "rfq": RFQ_REFERENCE,
    "soluble_protein": SOLUBLE_PROTEIN_REFERENCE,
    "npn": NPN_REFERENCE,
    "adip": ADIP_REFERENCE,

    # -----------------------------------------------------------------
    # NIR extra keys used in the NIR table definitions
    # -----------------------------------------------------------------
    "nfc_pct_db": NFC_REFERENCE,
    "starch_pct_db": STARCH_REFERENCE,
    "wsc_pct_db": WSC_REFERENCE,
    "fructan_pct_db": FRUCTAN_REFERENCE,
    "nel_pct_db": NEL_REFERENCE,
    "nem_pct_db": NEM_REFERENCE,
    "neg_pct_db": NEG_REFERENCE,
    "ash_pct_db": ASH_REFERENCE,
    "ca_pct_db": CA_NIR_REFERENCE,
    "p_pct_db": P_NIR_REFERENCE,
    "k_pct_db": K_NIR_REFERENCE,
    "mg_pct_db": MG_NIR_REFERENCE,
    "ndfd48_pctndf_db": NDFD48_REFERENCE,
    "ivtdmd48_pctndf_db": IVTDMD48_REFERENCE,
    "fat_pct_db": FAT_REFERENCE,
    "lignin_pct_db": LIGNIN_REFERENCE,
}