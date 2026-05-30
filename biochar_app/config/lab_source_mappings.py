
from typing import Dict
# -----------------------------------------------------------------------------

RAW_TO_CANONICAL_NIR: dict[str, str] = {
    # identifiers / dates
    "sample_id": "sample_id",
    "date_rec": "date_rec",
    "date_reported": "date_reported",

    # protein / moisture
    "crude_protein_pct_db": "crude_protein_pct_db",
    "moisture_pct": "moisture_pct",
    "dry_matter_pct": "dry_matter_pct",

    # fiber / energy
    "adf_pct_db": "adf_pct_db",
    "ndf_pct_db": "ndf_pct_db",
    "tdn_pct_db": "tdn_pct_db",
    "nel_pct_db": "nel_pct_db",
    "nem_pct_db": "nem_pct_db",
    "neg_pct_db": "neg_pct_db",
    "lignin_pct_db": "lignin_pct_db",

    # quality indices
    "RFV": "rfv",
    "RFQ": "rfq",

    # minerals
    "Ca_pct_db": "Ca_pct_db",
    "P_pct_db": "P_pct_db",
    "K_pct_db": "K_pct_db",
    "Mg_pct_db": "Mg_pct_db",

    # additional dry-basis fractions
    "ash_pct_db": "ash_pct_db",
    "ndfd48_pctndf_db": "ndfd48_pctndf_db",
    "ivtdmd48_pctndf_db": "ivtdmd48_pctndf_db",
    "fat_pct_db": "fat_pct_db",
    "nfc_pct_db": "nfc_pct_db",
    "starch_pct_db": "starch_pct_db",
    "esc_pct_db": "esc_pct_db",
    "wsc_pct_db": "wsc_pct_db",
    "fructan_pct_db": "fructan_pct_db",
}

DROP_COLUMNS_NIR = {
    "customer",
    "first_name",
    "last_name",
    "company",
    "address_1",
    "address_2",
    "city",
    "state",
    "zip",
    "date_reported",
    "lab_no",
    "results_for",
    "description",
    "moisture_pct_db",
    "dry_matter_pct_db",
}

EXPECTED_NIR_COLUMNS = [
    "strip",
    "nir_date",
]

RAW_TO_CANONICAL_SOILCHEM: dict[str, str] = {
    # identifiers / dates
    "sample_id_2": "strip",
    "date_received": "date_rec",
    "date_reported": "date_rept",

    # pH / salinity / lime
    "soil_ph_1_1": "soil_ph_1_1",
    "bph_modified_wdrf": "wdrf_buffer_ph",
    "soluble_salts_1_1_mmho_cm": "ec_1_1",
    "excess_lime_rating": "excess_lime",

    # organic matter / nutrients
    "organic_matter_loi_pct": "organic_matter_loi_pct",
    "nitrate_ppm_no3_n": "h2o_no3_n",
    "nh4_no3": "nh4_no3_ratio",
    "phosphorus_olsen_p_ppm_p": "olsen_p_ppm_p",
    "potassium_nh4oac_ppm_k": "potassium_ppm_k",
    "sulfate_m_3_ppm_s": "sulfate_s_ppm_s",

    # base cations
    "calcium_nh4oac_ppm_ca": "calcium_ppm_ca",
    "magnesium_nh4oac_ppm_mg": "magnesium_ppm_mg",
    "sodium_nh4oac_ppm_na": "sodium_ppm_na",
    "sum_of_cations_me_100g": "cec_sum_of_cations_me_100g",

    # base saturation
    "h_saturation_pct": "pcth_sat",
    "k_saturation_pct": "pctk_sat",
    "ca_saturation_pct": "pctca_sat",
    "mg_saturation_pct": "pctmg_sat",
    "na_saturation_pct": "pctna_sat",

    # micronutrients
    "zinc_dtpa_sorb_ppm_zn": "zinc_ppm_zn",
    "iron_dtpa_sorb_ppm_fe": "iron_ppm_fe",
    "manganese_dtpa_sorb_ppm_mn": "manganese_ppm_mn",
    "copper_dtpa_sorb_ppm_cu": "copper_ppm_cu",

    # SHA / soil health
    "ammonium_ppm_nh4_n": "h2o_nh4_n",
    "organic_carbon_ppm_c": "organic_c_h2o_ppm",
    "organic_nitrogen_ppm_n": "organic_n_h2o_ppm",
    "organic_c_n": "organic_c_n_h2o",
    "soil_health_calculation": "soil_health_score",
    "organic_nitrogen_release_ppm_n": "organic_nitrogen_release_ppm_n",
    "organic_nitrogen_reserve_ppm_n": "organic_nitrogen_reserve_ppm_n",
    "microbially_active_carbon_pct_mac": "microbially_active_carbon_pctma",
    "total_nitrogen_ppm_n": "total_n_h2o_ppm_n",
    "soil_respiration_ppm_co2c": "co2_soil_respiration",
    "water_stable_aggregates_mod_pct": "water_stable_aggregates_mod",

    # crop recs
    "yg_1": "yg_1",
    "nitrogen_rec_1": "nitrogen_rec",
    "p205_rec_1": "p2o5_rec",
    "k20_rec_1": "k2o_rec",
    "sulfur_rec_1": "sulfur_rec",
    "zinc_rec_1": "zinc_rec",
    "magnesium_rec_1": "magnesium_rec",
    "iron_rec_1": "iron_rec",
    "manganese_rec_1": "manganese_rec",
    "copper_rec_1": "copper_rec",

    # alternate fertilizer recommendation names, used by 2025-11-03 file
    "nitrogen_n": "nitrogen_rec",
    "phosphorus_p2o5": "p2o5_rec",
    "potassium_k2o": "k2o_rec",
    "sulfur_s": "sulfur_rec",
    "zinc_zn": "zinc_rec",
    "magnesium_mg": "magnesium_rec",
    "iron_fe": "iron_rec",
    "manganese_mn": "manganese_rec",
    "copper_cu": "copper_rec",
}

DROP_COLUMNS_SOILCHEM = {
    "sample_type",
    "customer_no",
    "name",
    "company",
    "address_1",
    "address_2",
    "city",
    "state",
    "zip",
    "lab_no",
    "results_for",
    "begin_depth",
    "end_depth",
    "sample_id_1",
    "sample_id_3",
    "crop",
    "past_crop",
    "cover_crop_suggestion_legume_grass",
    "boron_rec_1",
    "chlorine_rec_1",
    "lime_rec_1",
}

EXPECTED_SOILCHEM_COLUMNS = [
    "strip",
    "date_rec",
    "date_rept",
    "begin_depth_in",
    "end_depth_in",
]

# ---------------------------------------------------------------------
# Canonical soil-bio column mapping
# ---------------------------------------------------------------------
RAW_TO_CANONICAL_SOILBIO: Dict[str, str] = {
    # dates
    "date_recd": "date_rec",

    # biomass / percentages
    "total_biomass": "total_biomass",
    "total_bacteria_biomass": "total_bacteria_biomass",
    "bacteria_pct": "bacteria_pct",

    # duplicate "gram" headers: first is Gram (+), second becomes _1 and is Gram (-)
    "gram_biomass": "gram_pos_biomass",
    "gram_pct": "gram_pos_pct",

    "actinomycetes_biomass": "actinomycetes_biomass",
    "actinomycetes_pct": "actinomycetes_pct",

    "gram_biomass_1": "gram_neg_biomass",
    "gram_pct_1": "gram_neg_pct",

    # Explicit aliases used by some raw/supplemental files
    "gram_pos_biomass": "gram_pos_biomass",
    "gram_pos_pct": "gram_pos_pct",
    "gram_neg_biomass": "gram_neg_biomass",
    "gram_neg_pct": "gram_neg_pct",

    "rhizobia_biomass": "rhizobia_biomass",
    "rhizobia_pct": "rhizobia_pct",

    "total_fungi_biomass": "total_fungi_biomass",
    "total_fungi_pct": "total_fungi_pct",

    "arbuscular_mycorrhizal_biomass": "arbuscular_mycorrhizal_biomass",
    "arbuscular_mycorrhizal_pct": "arbuscular_mycorrhizal_pct",

    "saprophytic_pct": "saprophytic_pct",
    "saprophytes_biomass": "saprophytes_biomass",

    "protozoan_pct": "protozoan_pct",
    "protozoa_biomass": "protozoa_biomass",

    "undifferentiated_pct": "undifferentiated_pct",
    "undifferentiated_biomass": "undifferentiated_biomass",

    # ratios / lipid fractions
    "fungi_bacteria": "fungi_bacteria",
    "predator_prey": "predator_prey",
    "gram_gram": "gram_pos_gram_neg_ratio",
    "gram_pos_gram_neg": "gram_pos_gram_neg_ratio",
    "gram_pos_gram_neg_ratio": "gram_pos_gram_neg_ratio",

    "saturated": "saturated",
    "unsaturated": "unsaturated",
    "saturated_unsaturated": "saturated_unsaturated_ratio",
    "saturated_unsaturated_ratio": "saturated_unsaturated_ratio",
    "sat_unsat": "saturated_unsaturated_ratio",

    "monounsaturated": "monounsaturated",
    "polyunsaturated": "polyunsaturated",
    "monounsaturated_polyunsaturated": "monounsaturated_polyunsaturated_ratio",
    "monounsaturated_polyunsaturated_ratio": "monounsaturated_polyunsaturated_ratio",
    "mono_poly": "monounsaturated_polyunsaturated_ratio",

    "pre_16_1_w7c": "pre_16_1_w7c",
    "cyclo_17_0": "cyclo_17_0",
    "pre_16_1w7c_cy17_0": "pre_16_1w7c_cy17_0",

    "pre_18_1_w7c": "pre_18_1_w7c",
    "cyclo_19_0": "cyclo_19_0",
    "pre_18_1w7c_cy19_0": "pre_18_1w7c_cy19_0",

    "diversity_index": "diversity_index",
}

DROP_COLUMNS_SOILBIO = {
    "account_id",
    "address",
    "city",
    "st",
    "zip",
    "lab_id",
    "report_type",
}

EXPECTED_SOILBIO_COLUMNS = [
    "strip",
    "date_rec",
    "begin_depth_in",
    "end_depth_in",
    "total_biomass",
    "total_bacteria_biomass",
    "bacteria_pct",
    "gram_pos_biomass",
    "gram_pos_pct",
    "actinomycetes_biomass",
    "actinomycetes_pct",
    "gram_neg_biomass",
    "gram_neg_pct",
    "rhizobia_biomass",
    "rhizobia_pct",
    "total_fungi_biomass",
    "total_fungi_pct",
    "arbuscular_mycorrhizal_biomass",
    "arbuscular_mycorrhizal_pct",
    "saprophytic_pct",
    "saprophytes_biomass",
    "protozoan_pct",
    "protozoa_biomass",
    "undifferentiated_pct",
    "undifferentiated_biomass",
    "fungi_bacteria",
    "predator_prey",
    "gram_pos_gram_neg_ratio",
    "saturated",
    "unsaturated",
    "saturated_unsaturated_ratio",
    "monounsaturated",
    "polyunsaturated",
    "monounsaturated_polyunsaturated_ratio",
    "pre_16_1_w7c",
    "cyclo_17_0",
    "pre_16_1w7c_cy17_0",
    "pre_18_1_w7c",
    "cyclo_19_0",
    "pre_18_1w7c_cy19_0",
    "diversity_index",
]
