-- LTC LCS: Hypertension Register - Priority Group 1 (High Risk & Complex)
-- Parent population: Hypertension register
--
-- Inclusion: Latest BP in last 12 months exceeds severe thresholds
-- - Clinic BP: systolic > 180 OR diastolic > 120
-- - Home/ABPM BP: systolic > 170 OR diastolic > 115
--
-- Uses int_blood_pressure_latest which already pairs systolic/diastolic,
-- filters implausible values, and classifies home/ABPM reliably.

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Latest BP within 12 months with severe thresholds
-- Context-specific thresholds match EMIS logic:
-- Clinic: SBP > 180 or DBP > 120
-- Home/ABPM: SBP > 170 or DBP > 115
rule_1_severe_bp as (
    select person_id
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and (
          -- Clinic BP: severe if SBP > 180 or DBP > 120
          (not is_home_bp_event and not is_abpm_bp_event
           and (systolic_value > 180 or diastolic_value > 120))
          or
          -- Home/ABPM BP: severe if SBP > 170 or DBP > 115
          ((is_home_bp_event or is_abpm_bp_event)
           and (systolic_value > 170 or diastolic_value > 115))
      )
),

-- Combine rule results for all hypertension register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_severe_bp,
        case
            when r1.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    left join rule_1_severe_bp r1 on hr.person_id = r1.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
