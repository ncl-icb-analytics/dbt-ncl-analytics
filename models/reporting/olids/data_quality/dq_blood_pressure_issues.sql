{{
    config(
        materialized='table',
        tags=['data_quality', 'blood_pressure'],
        cluster_by=['person_id', 'effective_date'])
}}

/*
Blood Pressure Data Quality Issues
==================================
Identifies readings with data quality issues from int_blood_pressure_observations_base.
Aggregates to person/date level and flags specific issues.
*/

WITH aggregated_events AS (
    SELECT
        person_id,
        effective_date,

        -- Original values before any filtering
        MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) AS systolic_value_original,
        MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) AS diastolic_value_original,

        -- Metadata for traceability
        ANY_VALUE(result_unit_display) AS result_unit_display,
        ARRAY_AGG(DISTINCT id) WITHIN GROUP (ORDER BY id) AS all_ids,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids,

        -- Flags for specific code presence (for ambiguity checking)
        BOOLOR_AGG(source_cluster_id = 'SYSBP_COD') AS had_sysbp_cod,
        BOOLOR_AGG(source_cluster_id = 'DIASBP_COD') AS had_diabp_cod,
        
        -- DQ flags from base
        BOOLOR_AGG(had_future_date) AS had_future_date,
        BOOLOR_AGG(had_null_date) AS had_null_date

    FROM {{ ref('int_blood_pressure_observations_base') }}
    GROUP BY person_id, effective_date
    HAVING systolic_value_original IS NOT NULL OR diastolic_value_original IS NOT NULL
)

-- Final output: Only events with data quality issues
SELECT
    person_id,
    effective_date,
    systolic_value_original,
    diastolic_value_original,
    result_unit_display,
    all_concept_codes,
    all_concept_displays,
    all_source_cluster_ids,

    -- DQ Flag: Systolic out of range (< 40 or > 350)
    CASE
        WHEN systolic_value_original IS NOT NULL
             AND (systolic_value_original < 40 OR systolic_value_original > 350)
        THEN TRUE
        ELSE FALSE
    END AS is_sbp_out_of_range,

    -- DQ Flag: Diastolic out of range (< 20 or > 200)
    CASE
        WHEN diastolic_value_original IS NOT NULL
             AND (diastolic_value_original < 20 OR diastolic_value_original > 200)
        THEN TRUE
        ELSE FALSE
    END AS is_dbp_out_of_range,

    -- DQ Flag: Coding ambiguity (BP_COD without specific SYSBP/DIABP codes)
    CASE
        WHEN ARRAY_CONTAINS('BP_COD'::VARIANT, all_source_cluster_ids)
             AND NOT had_sysbp_cod AND NOT had_diabp_cod
        THEN TRUE
        ELSE FALSE
    END AS is_coding_ambiguous,

    -- DQ Flag: Orphaned reading (SBP without DBP or vice versa)
    CASE
        WHEN (systolic_value_original IS NOT NULL AND diastolic_value_original IS NULL)
             OR (systolic_value_original IS NULL AND diastolic_value_original IS NOT NULL)
        THEN TRUE
        ELSE FALSE
    END AS is_orphaned_reading,

    -- DQ Flag: Had future date (was corrected using date_recorded)
    had_future_date AS is_future_date_corrected,
    
    -- DQ Flag: Missing date
    had_null_date AS is_date_missing

FROM aggregated_events

-- Only include events with at least one DQ issue
WHERE (systolic_value_original IS NOT NULL AND (systolic_value_original < 40 OR systolic_value_original > 350))
   OR (diastolic_value_original IS NOT NULL AND (diastolic_value_original < 20 OR diastolic_value_original > 200))
   OR (ARRAY_CONTAINS('BP_COD'::VARIANT, all_source_cluster_ids) AND NOT had_sysbp_cod AND NOT had_diabp_cod)
   OR (systolic_value_original IS NOT NULL AND diastolic_value_original IS NULL)
   OR (systolic_value_original IS NULL AND diastolic_value_original IS NOT NULL)
   OR had_future_date = TRUE
   OR had_null_date = TRUE

ORDER BY person_id, effective_date DESC
