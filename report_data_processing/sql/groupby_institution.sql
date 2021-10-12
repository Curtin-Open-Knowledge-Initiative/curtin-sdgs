SELECT
    identifier,
    name,
    published_year,
    COUNT(doi) as total_outputs,
    COUNTIF(sdg_1_no_poverty) as sdg_1_no_poverty,
    COUNTIF(sdg_2_zero_hunger) as sdg_2_zero_hunger,
    COUNTIF(sdg_3_health_well_being) as sdg_3_health_well_being,
    COUNTIF(sdg_4_quality_education) as sdg_4_quality_education,
    COUNTIF(sdg_5_gender_equality) as sdg_5_gender_equality,
    COUNTIF(sdg_6_clean_water) as sdg_6_clean_water,
    COUNTIF(sdg_7_clean_energy) as sdg_7_clean_energy,
    COUNTIF(sdg_8_decent_work) as sdg_8_decent_work,
    COUNTIF(sdg_9_infrastructure_innovation) as sdg_9_infrastructure_innovation,
    COUNTIF(sdg_10_reduced_inequalities) as sdg_10_reduced_inequalities,
    COUNTIF(sdg_11_sustainable_cities) as sdg_11_sustainable_cities,
    COUNTIF(sdg_12_responsible_consumption) as sdg_12_responsible_consumption,
    COUNTIF(sdg_13_climate_action) as sdg_13_climate_action,
    COUNTIF(sdg_14_life_below_water) as sdg_14_life_below_water,
    COUNTIF(sdg_15_life_on_land) as sdg_15_life_on_land,
    COUNTIF(sdg_16_peace_institutions) as sdg_16_peace_institutions,
    COUNTIF(sdg_17_partnerships) as sdg_17_partnerships,
    COUNTIF(sustainab_title_abs_field) as sustainab_title_abs_field,
    COUNTIF(development_title_abs_field) as development_title_abs_field,
    COUNTIF(sdgs_ref_in_title_abs) as sdgs_ref_in_title_abs,

    COUNTIF(sdg_1_no_poverty AND sustainab_title_abs_field) as sdg_1_no_poverty_sustainab,
    COUNTIF(sdg_2_zero_hunger AND sustainab_title_abs_field) as sdg_2_zero_hunger_sustainab,
    COUNTIF(sdg_3_health_well_being AND sustainab_title_abs_field) as sdg_3_health_well_being_sustainab,
    COUNTIF(sdg_4_quality_education AND sustainab_title_abs_field) as sdg_4_quality_education_sustainab,
    COUNTIF(sdg_5_gender_equality AND sustainab_title_abs_field) as sdg_5_gender_equality_sustainab,
    COUNTIF(sdg_6_clean_water AND sustainab_title_abs_field) as sdg_6_clean_water_sustainab,
    COUNTIF(sdg_7_clean_energy AND sustainab_title_abs_field) as sdg_7_clean_energy_sustainab,
    COUNTIF(sdg_8_decent_work AND sustainab_title_abs_field) as sdg_8_decent_work_sustainab,
    COUNTIF(sdg_9_infrastructure_innovation AND sustainab_title_abs_field) as sdg_9_infrastructure_innovation_sustainab,
    COUNTIF(sdg_10_reduced_inequalities AND sustainab_title_abs_field) as sdg_10_reduced_inequalities_sustainab,
    COUNTIF(sdg_11_sustainable_cities AND sustainab_title_abs_field) as sdg_11_sustainable_cities_sustainab,
    COUNTIF(sdg_12_responsible_consumption AND sustainab_title_abs_field) as sdg_12_responsible_consumption_sustainab,
    COUNTIF(sdg_13_climate_action AND sustainab_title_abs_field) as sdg_13_climate_action_sustainab,
    COUNTIF(sdg_14_life_below_water AND sustainab_title_abs_field) as sdg_14_life_below_water_sustainab,
    COUNTIF(sdg_15_life_on_land AND sustainab_title_abs_field) as sdg_15_life_on_land_sustainab,
    COUNTIF(sdg_16_peace_institutions AND sustainab_title_abs_field) as sdg_16_peace_institutions_sustainab,
    COUNTIF(sdg_17_partnerships AND sustainab_title_abs_field) as sdg_17_partnerships_sustainab

FROM `coki-scratch-space.curtin.filtered_doi_sdgs`

GROUP BY identifier, name, published_year
ORDER BY name ASC, published_year DESC