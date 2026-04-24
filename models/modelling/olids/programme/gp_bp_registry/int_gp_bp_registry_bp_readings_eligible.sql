{{
    config(
        materialized='table',
        cluster_by=['person_id', 'effective_date'])
}}

/*
Blood pressure readings eligible for the GP BP Registry cohort.

Takes int_blood_pressure_all (paired systolic + diastolic events), restricts to persons
in the registry base population, and drops any reading dated inside a pregnancy or HDP
exclusion window.

One row per retained BP reading.
*/

SELECT
    bp.person_id,
    bp.effective_date,
    bp.systolic_value,
    bp.diastolic_value,
    bp.is_home_bp_event,
    bp.is_abpm_bp_event,
    bp.is_hypertensive_range

FROM {{ ref('int_blood_pressure_all') }} bp
INNER JOIN {{ ref('int_gp_bp_registry_base_population') }} pop
    ON bp.person_id = pop.person_id
LEFT JOIN {{ ref('int_hdp_exclusion_windows_all') }} w
    ON bp.person_id = w.person_id
    AND bp.effective_date BETWEEN w.window_start AND w.window_end

WHERE w.window_id IS NULL

ORDER BY bp.person_id, bp.effective_date
