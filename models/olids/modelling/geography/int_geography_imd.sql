{{
    config(
        materialized='table'
    )
}}

/*
Index of Multiple Deprivation (IMD) Mappings and Calculations
Provides LSOA-to-IMD mappings and quintile calculations from deciles.
Based on IMD 2019 data which uses 2011 LSOA boundaries.
*/

WITH imd_2019_data AS (
    -- IMD 2019 reference data
    SELECT
        lsoacode AS lsoa_code_2011,
        imddecile AS imd_decile
    FROM {{ ref('stg_reference_imd2019') }}
    WHERE lsoacode IS NOT NULL
        AND imddecile IS NOT NULL
)

SELECT
    lsoa_code_2011,
    imd_decile AS imd_decile_19,

    -- IMD Quintile calculation from deciles
    CASE
        WHEN imd_decile IN (1, 2) THEN 'Most Deprived'
        WHEN imd_decile IN (3, 4) THEN 'Second Most Deprived'
        WHEN imd_decile IN (5, 6) THEN 'Third Most Deprived'
        WHEN imd_decile IN (7, 8) THEN 'Second Least Deprived'
        WHEN imd_decile IN (9, 10) THEN 'Least Deprived'
        ELSE NULL
    END AS imd_quintile_19,

    -- Numeric quintile for easier filtering/sorting
    CASE
        WHEN imd_decile IN (1, 2) THEN 1
        WHEN imd_decile IN (3, 4) THEN 2
        WHEN imd_decile IN (5, 6) THEN 3
        WHEN imd_decile IN (7, 8) THEN 4
        WHEN imd_decile IN (9, 10) THEN 5
        ELSE NULL
    END AS imd_quintile_numeric_19,

    -- IMD Tertile calculation for additional grouping options
    CASE
        WHEN imd_decile IN (1, 2, 3) THEN 'Most Deprived Third'
        WHEN imd_decile IN (4, 5, 6, 7) THEN 'Middle Third'
        WHEN imd_decile IN (8, 9, 10) THEN 'Least Deprived Third'
        ELSE NULL
    END AS imd_tertile_19,

    -- Binary deprivation flag (most deprived 20% - top 2 deciles)
    CASE
        WHEN imd_decile IN (1, 2) THEN TRUE
        ELSE FALSE
    END AS is_most_deprived_20pct

FROM imd_2019_data
ORDER BY lsoa_code_2011