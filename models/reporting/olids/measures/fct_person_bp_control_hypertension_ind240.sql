{{ config(materialized='view') }}

-- NICE IND240: hypertension blood pressure control for people aged 80 years and over.
WITH indicator_population AS (
    SELECT
        register.person_id,
        age.age
    FROM {{ ref('fct_person_hypertension_register') }} AS register
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON register.person_id = age.person_id
    WHERE register.is_on_register = TRUE
        AND age.age >= 80
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        bp.latest_bp_date,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        bp.applied_measurement_context,
        COALESCE(
            bp.is_latest_bp_within_recommended_interval,
            FALSE
        ) AS is_bp_recorded_in_last_12m,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 145
            ELSE 150
        END AS indicator_systolic_threshold,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 85
            ELSE 90
        END AS indicator_diastolic_threshold
    FROM indicator_population AS population
    LEFT JOIN {{ ref('fct_person_bp_control') }} AS bp
        ON population.person_id = bp.person_id
),

status AS (
    SELECT
        *,
        COALESCE(
            latest_systolic_value < indicator_systolic_threshold
            AND latest_diastolic_value < indicator_diastolic_threshold,
            FALSE
        ) AS is_latest_bp_within_indicator_target
    FROM assessed
)

SELECT
    person_id,
    'IND240' AS indicator_id,
    'Hypertension: blood pressure (80 years and over)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    age,
    'Hypertension' AS condition_name,
    latest_bp_date,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    TRUE AS is_in_denominator,
    is_bp_recorded_in_last_12m,
    is_latest_bp_within_indicator_target,
    is_bp_recorded_in_last_12m
        AND is_latest_bp_within_indicator_target AS is_in_numerator,
    CASE
        WHEN NOT is_bp_recorded_in_last_12m THEN 'BP_NOT_RECORDED_IN_LAST_12M'
        WHEN is_latest_bp_within_indicator_target THEN 'ACHIEVED'
        ELSE 'BP_ABOVE_TARGET'
    END AS indicator_status
FROM status
