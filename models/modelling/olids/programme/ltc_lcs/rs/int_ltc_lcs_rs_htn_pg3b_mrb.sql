-- LTC LCS: Hypertension Register - Priority Group 3B (Medium Risk b)
-- Parent population: Hypertension register, excluding PG1, PG2, PG3A
--
-- No comorbidity required — just uncontrolled BP at age-appropriate thresholds.
--
-- Age <= 80:
-- - Clinic: SBP > 140 or DBP > 90
-- - Home/ABPM: SBP > 135 or DBP > 85
--
-- Age > 80 (relaxed systolic):
-- - Clinic: SBP > 150 or DBP > 90
-- - Home/ABPM: SBP > 145 or DBP > 85

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct
        hr.person_id,
        hr.age
    from {{ ref('fct_person_hypertension_register') }} hr
    where hr.is_on_register = true
),

-- Exclude PG1, PG2, PG3A
higher_pg_exclusions as (
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg3a_mra') }}
),

-- Must have any BP in last 12 months (gate)
has_recent_bp as (
    select distinct person_id
    from {{ ref('int_blood_pressure_all') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- Latest CLINIC BP in last 12 months
latest_clinic_bp as (
    select person_id, systolic_value, diastolic_value
    from {{ ref('int_blood_pressure_all') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and not coalesce(is_home_bp_event or is_abpm_bp_event, false)
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

-- Latest HOME/ABPM BP in last 12 months
latest_home_abpm_bp as (
    select person_id, systolic_value, diastolic_value
    from {{ ref('int_blood_pressure_all') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and (is_home_bp_event or is_abpm_bp_event)
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

-- EMIS evaluates clinic and home/ABPM BP independently.
-- Patient is controlled (excluded) if EITHER latest clinic OR latest home/ABPM is in range.
-- Uncontrolled = NOT controlled by clinic AND NOT controlled by home/ABPM.
--
-- Age <= 80: Clinic <=140/<=90, Home/ABPM <=135/<=85 → controlled
-- Age > 80:  Clinic <=150/<=90, Home/ABPM <=145/<=85 → controlled
controlled_bp as (
    select hr.person_id
    from hypertension_register hr
    left join latest_clinic_bp cbp on hr.person_id = cbp.person_id
    left join latest_home_abpm_bp hbp on hr.person_id = hbp.person_id
    where
        -- Controlled via clinic BP
        (cbp.person_id is not null and (
            (coalesce(hr.age, 0) <= 80 and cbp.systolic_value <= 140 and cbp.diastolic_value <= 90)
            or
            (coalesce(hr.age, 0) > 80 and cbp.systolic_value <= 150 and cbp.diastolic_value <= 90)
        ))
        or
        -- Controlled via home/ABPM BP
        (hbp.person_id is not null and (
            (coalesce(hr.age, 0) <= 80 and hbp.systolic_value <= 135 and hbp.diastolic_value <= 85)
            or
            (coalesce(hr.age, 0) > 80 and hbp.systolic_value <= 145 and hbp.diastolic_value <= 85)
        ))
),

-- Combine all rules
patient_rules as (
    select
        hr.person_id,
        (ctrl.person_id is null) as has_uncontrolled_bp,
        case
            when ctrl.person_id is null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    inner join has_recent_bp rbp on hr.person_id = rbp.person_id  -- Must have BP in last 12 months
    left join higher_pg_exclusions excl on hr.person_id = excl.person_id
    left join controlled_bp ctrl on hr.person_id = ctrl.person_id
    where excl.person_id is null  -- Exclude higher priority groups
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '3B' as priority_group,
    'MRb' as risk_group
from patient_rules
where final_status = 'Included'
