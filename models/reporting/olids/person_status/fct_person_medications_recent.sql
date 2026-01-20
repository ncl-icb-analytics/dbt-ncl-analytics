{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medications']
    )
}}

/*
Medications prescribed within the last 30 days and last year per person.

Person-level snapshot of recent medication prescriptions with counts.
Includes all medication orders (not just repeat prescriptions) from the last 30 days and last year.

Grain: One row per person with medications in last 30 days or last year
*/

WITH medication_orders_base AS (
    -- Get all medication orders from the last year
    SELECT
        pp.person_id,
        COALESCE(bnf.bnf_name, mo.medication_name, mo.statement_medication_name) AS medication_name,
        bnf.vtm AS vtm_code,
        bnf.vtm_name AS vtm_name,
        mo.clinical_effective_date AS order_date,
        CASE 
            WHEN mo.clinical_effective_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 1 
            ELSE 0 
        END AS is_last_30d
    FROM {{ ref('stg_olids_medication_order') }} mo
    INNER JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
        ON mo.mapped_concept_code = bnf.snomed_code
    WHERE mo.clinical_effective_date >= DATEADD(day, -365, CURRENT_DATE())
        AND mo.clinical_effective_date <= CURRENT_DATE()
        AND mo.clinical_effective_date IS NOT NULL
        AND COALESCE(bnf.bnf_name, mo.medication_name, mo.statement_medication_name) IS NOT NULL
),

medications_30d AS (
    -- Get all medication orders from the last 30 days
    SELECT
        person_id,
        medication_name,
        COUNT(*) AS prescription_count
    FROM medication_orders_base
    WHERE is_last_30d = 1
    GROUP BY person_id, medication_name
),

medications_12mo AS (
    -- Get all medication orders from the last year
    SELECT
        person_id,
        medication_name,
        COUNT(*) AS prescription_count
    FROM medication_orders_base
    GROUP BY person_id, medication_name
),

medication_arrays_30d AS (
    -- Create arrays of medication objects with counts for last 30 days
    SELECT
        person_id,
        ARRAY_COMPACT(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'medication_name', medication_name,
                'prescription_count', prescription_count
            )
        ) WITHIN GROUP (ORDER BY medication_name)) AS medications_recent_30d,
        SUM(prescription_count) AS total_prescriptions_30d,
        COUNT(DISTINCT medication_name) AS unique_medication_count_30d
    FROM medications_30d
    GROUP BY person_id
),

active_ingredients_30d AS (
    -- Count unique active ingredients (VTMs) for last 30 days
    SELECT
        person_id,
        COUNT(DISTINCT vtm_code) AS unique_active_ingredient_count_30d
    FROM medication_orders_base
    WHERE is_last_30d = 1
        AND vtm_code IS NOT NULL
    GROUP BY person_id
),

medication_arrays_12mo AS (
    -- Create arrays of medication objects with counts for last year
    SELECT
        person_id,
        ARRAY_COMPACT(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'medication_name', medication_name,
                'prescription_count', prescription_count
            )
        ) WITHIN GROUP (ORDER BY medication_name)) AS medications_recent_12mo,
        SUM(prescription_count) AS total_prescriptions_12mo,
        COUNT(DISTINCT medication_name) AS unique_medication_count_12mo
    FROM medications_12mo
    GROUP BY person_id
),

active_ingredients_12mo AS (
    -- Count unique active ingredients (VTMs) for last year
    SELECT
        person_id,
        COUNT(DISTINCT vtm_code) AS unique_active_ingredient_count_12mo
    FROM medication_orders_base
    WHERE vtm_code IS NOT NULL
    GROUP BY person_id
)

-- Combine both time periods
SELECT
    COALESCE(m30d.person_id, m12mo.person_id, ai30d.person_id, ai12mo.person_id) AS person_id,
    COALESCE(m30d.medications_recent_30d, ARRAY_CONSTRUCT()) AS medications_recent_30d,
    COALESCE(m30d.total_prescriptions_30d, 0) AS total_prescriptions_30d,
    COALESCE(m30d.unique_medication_count_30d, 0) AS unique_medication_count_30d,
    COALESCE(ai30d.unique_active_ingredient_count_30d, 0) AS unique_active_ingredient_count_30d,
    COALESCE(m12mo.medications_recent_12mo, ARRAY_CONSTRUCT()) AS medications_recent_12mo,
    COALESCE(m12mo.total_prescriptions_12mo, 0) AS total_prescriptions_12mo,
    COALESCE(m12mo.unique_medication_count_12mo, 0) AS unique_medication_count_12mo,
    COALESCE(ai12mo.unique_active_ingredient_count_12mo, 0) AS unique_active_ingredient_count_12mo
FROM medication_arrays_30d m30d
FULL OUTER JOIN medication_arrays_12mo m12mo
    ON m30d.person_id = m12mo.person_id
FULL OUTER JOIN active_ingredients_30d ai30d
    ON COALESCE(m30d.person_id, m12mo.person_id) = ai30d.person_id
FULL OUTER JOIN active_ingredients_12mo ai12mo
    ON COALESCE(m30d.person_id, m12mo.person_id, ai30d.person_id) = ai12mo.person_id