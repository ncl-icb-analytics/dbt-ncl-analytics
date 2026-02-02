{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        tags=['intermediate', 'clinical', 'blood_pressure', 'incremental'],
        cluster_by=['person_id', 'effective_date'])
}}

/*
Base Blood Pressure Observations - Incremental
================================================
Raw BP observations with row classification flags.
Shared foundation for int_blood_pressure_all and dq_blood_pressure_issues.

Incremental Strategy:
- Uses lds_start_date_time (LDS processing timestamp) to catch late-arriving data
- Merge on id to handle updates

Future Date Handling:
- If clinical_effective_date > date_recorded, use date_recorded as fallback
- Clinical date cannot be after the date it was recorded
*/

WITH base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date AS clinical_effective_date_raw,
        obs.date_recorded,
        obs.lds_start_date_time,
        obs.result_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIASBP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD'") }}) obs
    WHERE obs.result_value IS NOT NULL
    {% if is_incremental() %}
      AND obs.lds_start_date_time > (SELECT MAX(lds_start_date_time) FROM {{ this }})
    {% endif %}
)

SELECT
    id,
    person_id,
    
    -- Effective date with future date fallback
    CASE 
        WHEN clinical_effective_date_raw > date_recorded THEN date_recorded
        ELSE clinical_effective_date_raw
    END AS effective_date,
    
    -- Keep original values for audit/DQ
    clinical_effective_date_raw,
    date_recorded,
    lds_start_date_time,
    
    result_value,
    'mmHg' AS result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    
    -- BP type classification flags
    (source_cluster_id = 'SYSBP_COD' OR
     (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%systolic%')) AS is_systolic_row,
    
    (source_cluster_id = 'DIASBP_COD' OR
     (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%diastolic%')) AS is_diastolic_row,
    
    -- Clinical context flags
    (source_cluster_id IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS is_home_bp_row,
    (source_cluster_id = 'ABPM_COD') AS is_abpm_bp_row,
    
    -- DQ flags for downstream filtering
    (clinical_effective_date_raw > date_recorded) AS had_future_date,
    (clinical_effective_date_raw IS NULL) AS had_null_date

FROM base_observations
