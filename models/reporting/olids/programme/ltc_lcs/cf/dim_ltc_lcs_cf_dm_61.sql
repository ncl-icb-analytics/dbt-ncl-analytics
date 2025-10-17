{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS CF DM_61 case finding
-- Patients at highest risk of diabetes (spec priority a):
-- Latest HbA1c ≥48 mmol/mol AND no diagnosis of diabetes or diabetes in remission/resolved

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

hba1c_readings AS (
    -- Get all HbA1c readings with values > 0
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
),

latest_hba1c AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_hba1c_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_hba1c_displays
    FROM hba1c_readings
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
),

diabetes_diagnoses AS (
    -- Get patients with diabetes diagnosis or remission/resolved codes
    SELECT DISTINCT
        person_id,
        TRUE AS has_diabetes_diagnosis
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id IN (
        'DIABETES_RESOLVED',
        'DIABETES_RESOLVED_COD',
        'DIABETES_IN_REMISSION'
    )
)

-- Final selection: HbA1c ≥48 without diabetes diagnosis/remission
SELECT
    bp.person_id,
    bp.age,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    hba1c.all_hba1c_codes,
    hba1c.all_hba1c_displays,
    COALESCE(dd.has_diabetes_diagnosis, FALSE) AS has_diabetes_diagnosis,
    COALESCE(
        hba1c.latest_hba1c_value >= 48
        AND COALESCE(dd.has_diabetes_diagnosis, FALSE) = FALSE,
        FALSE
    ) AS has_high_diabetes_risk
FROM base_population AS bp
INNER JOIN latest_hba1c AS hba1c ON bp.person_id = hba1c.person_id
LEFT JOIN diabetes_diagnoses AS dd ON bp.person_id = dd.person_id
WHERE
    hba1c.latest_hba1c_value >= 48
    AND COALESCE(dd.has_diabetes_diagnosis, FALSE) = FALSE
