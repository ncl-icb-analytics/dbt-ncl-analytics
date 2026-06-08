{% macro get_ltc_lcs_htn_bp_control(window_start, window_end) %}
-- LTC LCS Outcomes: Hypertension good blood pressure control (window-parameterised).
--
-- One row per person on the hypertension register, flagged for whether their LAST KNOWN
-- paired BP reading WITHIN the supplied window meets the strict good-control thresholds:
--   - age <= 80: systolic <= 140 AND diastolic <= 90
--   - age >  80: systolic <= 150 AND diastolic <= 90
--
-- Deliberately simplified vs NICE/NG136: NO clinic-vs-home/ABPM split and NO ACR/CKD/diabetes
-- target tightening. Every reading is scored on the same clinic thresholds. (fct_person_bp_control
-- does the full NG136 logic — this outcome is intentionally the simpler QOF-style good-control rule.)
--
-- window_start / window_end are SQL date expressions injected into the BP date filter, so the
-- same logic serves the rolling and FY variants. The macro itself stays window-agnostic.
with hypertension_register as (
    select
        person_id,
        age
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Last known paired reading within the window. Tie-break mirrors int_blood_pressure_latest
-- (lowest-of-day) so a multi-reading final date resolves deterministically.
bp_in_window as (
    select
        person_id,
        effective_date as latest_bp_date,
        systolic_value as latest_systolic_value,
        diastolic_value as latest_diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event
    from {{ ref('int_blood_pressure_all') }}
    where effective_date >= ({{ window_start }})
      and effective_date <= ({{ window_end }})
      and systolic_value is not null
      and diastolic_value is not null
    qualify row_number() over (
        partition by person_id
        order by effective_date desc, systolic_value asc, diastolic_value asc
    ) = 1
),

control as (
    select
        hr.person_id,
        hr.age,
        bp.latest_bp_date,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        -- Strict, age-based thresholds; diastolic target is 90 for both age bands.
        case when hr.age > 80 then 150 else 140 end as systolic_threshold,
        90 as diastolic_threshold,
        (bp.person_id is not null) as has_bp_in_window
    from hypertension_register hr
    left join bp_in_window bp
        on hr.person_id = bp.person_id
)

select
    person_id,
    age,
    latest_bp_date,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    systolic_threshold,
    diastolic_threshold,
    has_bp_in_window,
    -- Good control: both systolic AND diastolic at/below target (QOF <= semantics).
    -- Register members with no reading in window are uncontrolled (false), flagged via has_bp_in_window.
    coalesce(
        latest_systolic_value <= systolic_threshold
        and latest_diastolic_value <= diastolic_threshold,
        false
    ) as is_bp_controlled
from control
{% endmacro %}
