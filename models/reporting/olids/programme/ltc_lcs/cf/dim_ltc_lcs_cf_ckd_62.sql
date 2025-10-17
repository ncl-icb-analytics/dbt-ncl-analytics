/*
LTC LCS Case Finding: CKD_62 - Multiple High UACR Readings Without Subsequent Normal

Purpose:
- Identifies patients with two readings of uACR>4 and no subsequent normal reading

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)

2. UACR Criteria:
   - Must have at least 2 UACR readings >4
   - Takes max value per day to handle multiple readings
   - Filters out adjacent day duplicates (same result on consecutive days)
   - Most recent UACR must also be >4 (no subsequent normal reading)
   - Uses 'UACR_TESTING' cluster ID

3. Output:
   - Only includes patients with 2+ high readings and no subsequent normal
   - Provides latest UACR value and date
   - Collects all UACR concept codes and displays for traceability

Implementation Notes:
- Materialized as table
- Implements adjacent day filtering logic
- Checks latest reading is high to ensure no subsequent normal

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For UACR readings

*/

{{ config(
    materialized='table') }}

WITH base_population AS (
    -- Get base population of patients over 17
    -- Base population already excludes those on CKD and Diabetes registers
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

uacr_readings AS (
    -- Get all UACR readings with values > 0
    -- Take max value per day to handle multiple readings
    SELECT
        person_id,
        clinical_effective_date,
        max(cast(result_value AS number)) AS result_value,
        any_value(mapped_concept_code) AS concept_code,
        any_value(mapped_concept_display) AS concept_display
    FROM {{ ref('int_ltc_lcs_ckd_observations') }}
    WHERE
        cluster_id = 'UACR_TESTING'
        AND result_value IS NOT NULL
        AND cast(result_value AS number) > 0
    GROUP BY
        person_id,
        clinical_effective_date
),

uacr_with_adjacent_check AS (
    -- Check for same results on adjacent days
    SELECT
        *,
        CASE
            WHEN
                dateadd(DAY, 1, clinical_effective_date)
                = lag(clinical_effective_date)
                    OVER (
                        PARTITION BY person_id
                        ORDER BY clinical_effective_date DESC
                    )
                AND result_value
                = lag(result_value)
                    OVER (
                        PARTITION BY person_id
                        ORDER BY clinical_effective_date DESC
                    )
                THEN 'EXCLUDE'
            ELSE 'INCLUDE'
        END AS adjacent_day_check
    FROM uacr_readings
),

uacr_filtered AS (
    -- Remove adjacent day duplicates
    SELECT *
    FROM uacr_with_adjacent_check
    WHERE adjacent_day_check = 'INCLUDE'
),

uacr_ranked AS (
    -- Rank UACR readings by date for each person
    SELECT
        *,
        row_number()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS reading_rank
    FROM uacr_filtered
),

uacr_counts AS (
    -- Count readings per person to ensure at least 2
    SELECT
        person_id,
        count(*) AS reading_count
    FROM uacr_filtered
    GROUP BY person_id
    HAVING count(*) > 1
),

uacr_high_readings AS (
    -- Get patients with at least 2 readings >4
    SELECT
        person_id,
        clinical_effective_date,
        result_value
    FROM uacr_filtered
    WHERE result_value > 4
),

uacr_high_counts AS (
    -- Count high readings per person
    SELECT
        person_id,
        count(*) AS high_reading_count
    FROM uacr_high_readings
    GROUP BY person_id
    HAVING count(*) >= 2
),

latest_uacr_all AS (
    -- Get the most recent UACR (any value) for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_uacr_date,
        result_value AS latest_uacr_value
    FROM uacr_filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
),

uacr_with_lags AS (
    -- Get patients with 2+ high readings where latest reading is also >4 (no subsequent normal)
    SELECT
        hc.person_id,
        latest.latest_uacr_date,
        latest.latest_uacr_value,
        NULL AS previous_uacr_date,
        NULL AS previous_uacr_value
    FROM uacr_high_counts AS hc
    INNER JOIN latest_uacr_all AS latest ON hc.person_id = latest.person_id
    WHERE latest.latest_uacr_value > 4  -- No subsequent normal reading
),

uacr_codes AS (
-- Get all codes and displays for each person
    SELECT
        person_id,
        array_agg(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_uacr_codes,
        array_agg(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_uacr_displays
    FROM uacr_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    ceg.latest_uacr_date,
    ceg.previous_uacr_date,
    ceg.latest_uacr_value,
    ceg.previous_uacr_value,
    codes.all_uacr_codes,
    codes.all_uacr_displays,
    COALESCE(ceg.latest_uacr_value > 4, FALSE) AS has_elevated_uacr,
    COALESCE(ceg.latest_uacr_value > 4, FALSE) AS meets_criteria
FROM base_population AS bp
INNER JOIN uacr_with_lags AS ceg ON bp.person_id = ceg.person_id
LEFT JOIN uacr_codes AS codes ON bp.person_id = codes.person_id
