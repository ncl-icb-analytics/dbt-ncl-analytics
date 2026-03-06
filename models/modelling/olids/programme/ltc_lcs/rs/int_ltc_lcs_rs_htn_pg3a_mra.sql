-- LTC LCS: Hypertension Register - Priority Group 3A (Medium Risk a)
-- Parent population: Hypertension register, excluding PG1 (HRC) and PG2 (HR)
--
-- Inclusion: Has comorbidity AND latest BP in last 12 months exceeds Stage 1 thresholds
-- - Clinic: SBP > 140 or DBP > 90
-- - Home/ABPM: SBP > 135 or DBP > 85
--
-- Comorbidities (any one, using EMIS valuesets):
-- - vs3: CHD (CHD_COD, first episode excl review/end)
-- - vs4+vs5: Stroke/TIA (STRK_COD + TIA_COD, first episode excl review/end)
-- - vs6: PAD (PAD_COD, first episode excl review/end)
-- - vs7: CKD (CKD_COD)
-- - vs8: eGFR < 60
-- - vs9: Diabetes (DM_COD)
-- - vs10: Black ethnicity

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Exclude PG1 and PG2
higher_pg_exclusions as (
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
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

-- Rule 3: Comorbidities (any one qualifies)
-- CHD (first episode, excluding review/end)
rule_3_chd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs3") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- Stroke/TIA (first episode, excluding review/end)
rule_3_stroke_tia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs4,on_htn_reg_priority_group_3a_mra_v3_vs5") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- PAD (first episode, excluding review/end)
rule_3_pad as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs6") }})
    where is_problem = true
      and coalesce(is_review, false) = false
),

-- CKD
rule_3_ckd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs7") }})
),

-- eGFR < 60
rule_3_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_htn_reg_priority_group_3a_mra_v3_vs8") }})
    where result_value < 60
),

-- Diabetes
rule_3_diabetes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs9") }})
),

-- Black ethnicity
rule_3_ethnicity as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs10") }})
),

comorbidities as (
    select person_id from rule_3_chd
    union
    select person_id from rule_3_stroke_tia
    union
    select person_id from rule_3_pad
    union
    select person_id from rule_3_ckd
    union
    select person_id from rule_3_egfr
    union
    select person_id from rule_3_diabetes
    union
    select person_id from rule_3_ethnicity
),

-- Rules 4+5: BP exceeds Stage 1 thresholds
-- Clinic: SBP > 140 or DBP > 90
-- Home/ABPM: SBP > 135 or DBP > 85
uncontrolled_bp as (
    select person_id
    from latest_bp
    where (
        (not is_home_or_abpm and (systolic_value > 140 or diastolic_value > 90))
        or
        (is_home_or_abpm and (systolic_value > 135 or diastolic_value > 85))
    )
),

-- Combine all rules
patient_rules as (
    select
        hr.person_id,
        (com.person_id is not null) as has_comorbidity,
        (ubp.person_id is not null) as has_uncontrolled_bp,
        case
            when com.person_id is not null and ubp.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    inner join latest_bp lbp on hr.person_id = lbp.person_id  -- Must have BP in last 12 months
    left join higher_pg_exclusions excl on hr.person_id = excl.person_id
    left join comorbidities com on hr.person_id = com.person_id
    left join uncontrolled_bp ubp on hr.person_id = ubp.person_id
    where excl.person_id is null  -- Exclude PG1 and PG2
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '3A' as priority_group,
    'MRa' as risk_group
from patient_rules
where final_status = 'Included'
