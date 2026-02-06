{{
    config(
        materialized='table',
        tags=['intermediate', 'clinical', 'blood_pressure'],
        cluster_by=['person_id', 'effective_date'])
}}

/*
Blood Pressure Events - Event-based consolidation (one row per person per date)
===============================================================================
Aggregates from int_blood_pressure_observations_base for valid BP events.

Clinical validation:
- Both systolic and diastolic values present
- Systolic: 40-350 mmHg
- Diastolic: 20-200 mmHg
- Initial broad filter: 20-350 applied in base
*/

WITH valid_observations AS (
    SELECT *
    FROM {{ ref('int_blood_pressure_observations_base') }}
    WHERE effective_date IS NOT NULL
      -- Apply broad plausible value filter
      AND result_value > 20
      AND result_value < 350
      AND effective_date <= CURRENT_DATE() -- Exclude future dates
)

-- Event-level aggregation: one row per person per date with paired values
SELECT
    person_id,
    effective_date,
    effective_date AS clinical_effective_date,  -- Backward compatibility alias

    -- Consolidated BP values: pivot systolic/diastolic for the event date
    MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) AS systolic_value,
    MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) AS diastolic_value,

    -- Clinical context flags: if any observation on this date was Home/ABPM
    BOOLOR_AGG(is_home_bp_row) AS is_home_bp_event,
    BOOLOR_AGG(is_abpm_bp_row) AS is_abpm_bp_event,

    -- Stage 1 hypertension flag: ≥140/90 (clinic) or ≥135/85 (home/ABPM)
    CASE
        WHEN BOOLOR_AGG(is_home_bp_row) OR BOOLOR_AGG(is_abpm_bp_row) THEN
            (MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) >= 135
             OR MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) >= 85)
        ELSE
            (MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) >= 140
             OR MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) >= 90)
    END AS is_hypertensive_range,

    -- Traceability metadata for audit and debugging
    ANY_VALUE(result_unit_display) AS result_unit_display,
    MAX(CASE WHEN is_systolic_row THEN id ELSE NULL END) AS systolic_observation_id,
    MAX(CASE WHEN is_diastolic_row THEN id ELSE NULL END) AS diastolic_observation_id,
    ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
    ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
    ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids

FROM valid_observations
GROUP BY person_id, effective_date

-- Clinical validation: ensure we have valid paired BP events with plausible ranges
HAVING MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) IS NOT NULL
   AND MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) IS NOT NULL
   AND MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) >= 40 
   AND MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) <= 350
   AND MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) >= 20 
   AND MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) <= 200

ORDER BY person_id, effective_date DESC
