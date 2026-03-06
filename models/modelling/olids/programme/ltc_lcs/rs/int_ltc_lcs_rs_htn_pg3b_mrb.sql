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

-- Latest BP within 12 months
latest_bp as (
    select
        person_id,
        systolic_value,
        diastolic_value,
        coalesce(is_home_bp_event or is_abpm_bp_event, false) as is_home_or_abpm
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- Age-appropriate BP thresholds
-- Rules 3-4 (all ages): Clinic <=140/<=90, Home/ABPM <=135/<=85 → controlled
-- Rules 5-7 (age >80): Clinic <=150/<=90, Home/ABPM <=145/<=85 → controlled
uncontrolled_bp as (
    select
        lbp.person_id
    from latest_bp lbp
    inner join hypertension_register hr on lbp.person_id = hr.person_id
    where (
        -- Age <= 80: standard thresholds
        (coalesce(hr.age, 0) <= 80 and (
            (not lbp.is_home_or_abpm and (lbp.systolic_value > 140 or lbp.diastolic_value > 90))
            or
            (lbp.is_home_or_abpm and (lbp.systolic_value > 135 or lbp.diastolic_value > 85))
        ))
        or
        -- Age > 80: relaxed systolic thresholds
        (coalesce(hr.age, 0) > 80 and (
            (not lbp.is_home_or_abpm and (lbp.systolic_value > 150 or lbp.diastolic_value > 90))
            or
            (lbp.is_home_or_abpm and (lbp.systolic_value > 145 or lbp.diastolic_value > 85))
        ))
    )
),

-- Combine all rules
patient_rules as (
    select
        hr.person_id,
        (ubp.person_id is not null) as has_uncontrolled_bp,
        case
            when ubp.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    inner join latest_bp lbp on hr.person_id = lbp.person_id  -- Must have BP in last 12 months
    left join higher_pg_exclusions excl on hr.person_id = excl.person_id
    left join uncontrolled_bp ubp on hr.person_id = ubp.person_id
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
