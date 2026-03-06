-- LTC LCS: Hypertension Register - Priority Group 2 (High Risk)
-- Parent population: Hypertension register, excluding PG1 (HRC)
--
-- Included if PG2a OR PG2b criteria met:
--
-- PG2a: Stage 2 BP thresholds (no comorbidity required)
-- - Clinic: SBP > 160 or DBP > 100
-- - Home/ABPM: SBP > 150 or DBP > 95
--
-- PG2b: Stage 1 BP + Black ethnicity + comorbidity
-- - Clinic: SBP >= 140 or DBP >= 90
-- - Home/ABPM: SBP >= 135 or DBP >= 85
-- - Ethnicity: vs3 Black African/Caribbean/British
-- - Comorbidity (any): vs4 CHD, vs5+vs6 Stroke/TIA, vs7 PAD, vs8 CKD,
--   vs9 eGFR < 60, vs10 Diabetes, vs11 BMI > 35

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Exclude PG1
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
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

-- PG2a: Stage 2 hypertension thresholds
-- Clinic: SBP > 160 or DBP > 100
-- Home/ABPM: SBP > 150 or DBP > 95
pg2a_stage2_bp as (
    select person_id
    from latest_bp
    where (
        (not is_home_or_abpm and (systolic_value > 160 or diastolic_value > 100))
        or
        (is_home_or_abpm and (systolic_value > 150 or diastolic_value > 95))
    )
),

-- PG2b: Stage 1 BP thresholds
-- Clinic: SBP >= 140 or DBP >= 90
-- Home/ABPM: SBP >= 135 or DBP >= 85
pg2b_stage1_bp as (
    select person_id
    from latest_bp
    where (
        (not is_home_or_abpm and (systolic_value >= 140 or diastolic_value >= 90))
        or
        (is_home_or_abpm and (systolic_value >= 135 or diastolic_value >= 85))
    )
),

-- PG2b: Black ethnicity (EMIS valueset)
pg2b_ethnicity as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs3") }})
),

-- PG2b: Comorbidities (any one qualifies, using EMIS valuesets)
-- CHD (first episode, excluding review/end)
pg2b_chd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs4") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- Stroke/TIA (first episode, excluding review/end)
pg2b_stroke_tia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs5") }})
    where is_problem = true
      and coalesce(is_review, false) = false
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs6") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- PAD (first episode, excluding review/end)
pg2b_pad as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs7") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- CKD
pg2b_ckd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs8") }})
),

-- eGFR < 60
pg2b_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("priority_group_2b_icb_v3_vs9") }})
    where result_value < 60
),

-- Diabetes
pg2b_diabetes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs10") }})
),

-- BMI > 35
pg2b_bmi as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("priority_group_2b_icb_v3_vs11") }})
    where result_value > 35
),

pg2b_comorbidities as (
    select person_id from pg2b_chd
    union
    select person_id from pg2b_stroke_tia
    union
    select person_id from pg2b_pad
    union
    select person_id from pg2b_ckd
    union
    select person_id from pg2b_egfr
    union
    select person_id from pg2b_diabetes
    union
    select person_id from pg2b_bmi
),

-- PG2b combined: Stage 1 BP + Black ethnicity + comorbidity
pg2b_combined as (
    select bp.person_id
    from pg2b_stage1_bp bp
    inner join pg2b_ethnicity eth on bp.person_id = eth.person_id
    inner join pg2b_comorbidities com on bp.person_id = com.person_id
),

-- Combine: PG2a OR PG2b
patient_rules as (
    select
        hr.person_id,
        (a.person_id is not null) as pg2a_stage2_bp,
        (b.person_id is not null) as pg2b_combined,
        case
            when a.person_id is not null or b.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    inner join latest_bp lbp on hr.person_id = lbp.person_id  -- Must have BP in last 12 months
    left join pg1_exclusions pg1 on hr.person_id = pg1.person_id
    left join pg2a_stage2_bp a on hr.person_id = a.person_id
    left join pg2b_combined b on hr.person_id = b.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
