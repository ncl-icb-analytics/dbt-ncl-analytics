{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IMD2025_ICB \ndbt: source(''reference_analyst_managed'', ''IMD2025_ICB'') \nColumns:\n  INTEGRATED_CARE_BOARD_CODE_2024 -> integrated_care_board_code_2024\n  INTEGRATED_CARE_BOARD_NAME_2024 -> integrated_care_board_name_2024\n  IMD__AVERAGE_RANK -> imd_average_rank\n  IMD__RANK_OF_AVERAGE_RANK -> imd_rank_of_average_rank\n  IMD__AVERAGE_SCORE -> imd_average_score\n  IMD__RANK_OF_AVERAGE_SCORE -> imd_rank_of_average_score\n  IMD__PROPORTION_OF_LSOAS_IN_MOST_DEPRIVED_10_PERCENT_NATIONALLY -> imd_proportion_of_lsoas_in_most_deprived_10_percent_nationally\n  IMD__RANK_OF_PROPORTION_OF_LSOAS_IN_MOST_DEPRIVED_10_PERCENT_NATIONALLY -> imd_rank_of_proportion_of_lsoas_in_most_deprived_10_percent_nationally\n  IMD_2025__EXTENT -> imd_2025_extent\n  IMD_2025__RANK_OF_EXTENT -> imd_2025_rank_of_extent\n  IMD_2025__LOCAL_CONCENTRATION -> imd_2025_local_concentration\n  IMD_2025__RANK_OF_LOCAL_CONCENTRATION -> imd_2025_rank_of_local_concentration"
    )
}}
select
    "INTEGRATED_CARE_BOARD_CODE_2024" as integrated_care_board_code_2024,
    "INTEGRATED_CARE_BOARD_NAME_2024" as integrated_care_board_name_2024,
    "IMD__AVERAGE_RANK" as imd_average_rank,
    "IMD__RANK_OF_AVERAGE_RANK" as imd_rank_of_average_rank,
    "IMD__AVERAGE_SCORE" as imd_average_score,
    "IMD__RANK_OF_AVERAGE_SCORE" as imd_rank_of_average_score,
    "IMD__PROPORTION_OF_LSOAS_IN_MOST_DEPRIVED_10_PERCENT_NATIONALLY" as imd_proportion_of_lsoas_in_most_deprived_10_percent_nationally,
    "IMD__RANK_OF_PROPORTION_OF_LSOAS_IN_MOST_DEPRIVED_10_PERCENT_NATIONALLY" as imd_rank_of_proportion_of_lsoas_in_most_deprived_10_percent_nationally,
    "IMD_2025__EXTENT" as imd_2025_extent,
    "IMD_2025__RANK_OF_EXTENT" as imd_2025_rank_of_extent,
    "IMD_2025__LOCAL_CONCENTRATION" as imd_2025_local_concentration,
    "IMD_2025__RANK_OF_LOCAL_CONCENTRATION" as imd_2025_rank_of_local_concentration
from {{ source('reference_analyst_managed', 'IMD2025_ICB') }}
