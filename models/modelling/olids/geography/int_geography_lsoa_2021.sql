{{
    config(
        materialized='table'
    )
}}

/*
LSOA 2021 geography with IMD 2025 deprivation data.
*/

WITH imd_2025_data AS (
    SELECT
        lsoa_code_2021,
        index_of_multiple_deprivation_decile AS imd_decile
    FROM {{ ref('stg_reference_imd2025') }}
    WHERE lsoa_code_2021 IS NOT NULL
        AND index_of_multiple_deprivation_decile IS NOT NULL
)

SELECT
    lsoa_code_2021,
    imd_decile AS imd_decile_25,

    -- IMD Quintile calculation from deciles
    CASE
        WHEN imd_decile IN (1, 2) THEN 'Most Deprived'
        WHEN imd_decile IN (3, 4) THEN 'Second Most Deprived'
        WHEN imd_decile IN (5, 6) THEN 'Third Most Deprived'
        WHEN imd_decile IN (7, 8) THEN 'Second Least Deprived'
        WHEN imd_decile IN (9, 10) THEN 'Least Deprived'
        ELSE NULL
    END AS imd_quintile_25,

    -- Numeric quintile for easier filtering/sorting
    CASE
        WHEN imd_decile IN (1, 2) THEN 1
        WHEN imd_decile IN (3, 4) THEN 2
        WHEN imd_decile IN (5, 6) THEN 3
        WHEN imd_decile IN (7, 8) THEN 4
        WHEN imd_decile IN (9, 10) THEN 5
        ELSE NULL
    END AS imd_quintile_numeric_25,

    -- IMD Tertile calculation for additional grouping options
    CASE
        WHEN imd_decile IN (1, 2, 3) THEN 'Most Deprived Third'
        WHEN imd_decile IN (4, 5, 6, 7) THEN 'Middle Third'
        WHEN imd_decile IN (8, 9, 10) THEN 'Least Deprived Third'
        ELSE NULL
    END AS imd_tertile_25,

    -- Binary deprivation flag (most deprived 20% - top 2 deciles)
    CASE
        WHEN imd_decile IN (1, 2) THEN TRUE
        ELSE FALSE
    END AS is_most_deprived_20pct_25

FROM imd_2025_data
ORDER BY lsoa_code_2021