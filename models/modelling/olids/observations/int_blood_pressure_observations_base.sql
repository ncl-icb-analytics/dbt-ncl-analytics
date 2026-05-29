{{
    config(
        materialized='incremental',
        unique_key=['id', 'source_cluster_id'],
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
- Uses lds_start_datetime (LDS processing timestamp) to catch late-arriving data
- Merge on id to handle updates

Note: Future date handling (clinical_effective_date > date_recorded) is applied
in the get_observations macro, benefiting all models that use it.
*/

WITH base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,  -- Already corrected by macro if needed
        obs.clinical_effective_date_raw,  -- Original value from macro
        obs.date_recorded,
        obs.lds_start_datetime,
        obs.result_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        -- Parent observation links systolic + diastolic of the same reading.
        -- Used downstream to pair within a reading rather than by date.
        src.parent_observation_id
    FROM ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIASBP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD'") }}) obs
    LEFT JOIN {{ ref('stg_olids_observation') }} src
        ON obs.id = src.id
    WHERE obs.result_value IS NOT NULL
      AND obs.person_id IS NOT NULL
    {% if is_incremental() %}
      -- COALESCE so the first incremental run (target empty) doesn't compare
      -- against NULL and exclude every row; '1900-01-01' is safely earlier
      -- than any real lds_start_datetime.
      -- Alias the target as `t` so Snowflake doesn't treat the subquery's
      -- bare `lds_start_datetime` as correlated against outer obs.
      AND obs.lds_start_datetime > COALESCE(
        (SELECT MAX(t.lds_start_datetime) FROM {{ this }} AS t),
        '1900-01-01'::TIMESTAMP_NTZ
      )
    {% endif %}
)

SELECT
    id,
    person_id,
    clinical_effective_date AS effective_date,
    
    -- Keep original values for audit/DQ
    clinical_effective_date_raw,
    date_recorded,
    lds_start_datetime,
    
    result_value,
    'mmHg' AS result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    parent_observation_id,

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
