-- Raw layer model for reference_analyst_managed.IMD2025_ICB
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
