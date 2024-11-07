from pathlib import Path

DOI_TABLE = 'academic-observatory.observatory.doi20231217'
TRUTH_TABLE = 'coki-scratch-space.curtin.doi_sdgs_2023'
JOIN_TABLE = 'coki-scratch-space.curtin.filtered_doi_sdgs_2023'
RERUN = True
VERBOSE = True

# Analysis variables
CURTIN_ROR = 'https://ror.org/02n415q13'

# Replace with applicable project name
PROJECT_ID = 'coki-scratch-space'
TEMPDIR = Path('tempdir')
DATA_FOLDER = Path('data')
SQL_DIRECTORY = Path('report_data_processing') / 'sql'

SDG_LIST = [
    'sdg_1_no_poverty',
    'sdg_2_zero_hunger',
    'sdg_3_health_well_being',
    'sdg_4_quality_education',
    'sdg_5_gender_equality',
    'sdg_6_clean_water',
    'sdg_7_clean_energy',
    'sdg_8_decent_work',
    'sdg_9_infrastructure_innovation',
    'sdg_10_reduced_inequalities',
    'sdg_11_sustainable_cities',
    'sdg_12_responsible_consumption',
    'sdg_13_climate_action',
    'sdg_14_life_below_water',
    'sdg_15_life_on_land',
    'sdg_16_peace_institutions',
    'sdg_17_partnerships'
]