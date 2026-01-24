-- LTC LCS: CKD Register - Priority Group 3 (Moderate Risk)
-- Parent population: CKD register, excluding PG1 (HRC) and PG2 (HR)
--
-- Inclusion rules (any one qualifies):
-- - Rule 2: eGFR > 60 AND ACR >= 30
-- - Rule 3: On Diabetes Register AND eGFR > 60 AND ACR >= 3
-- - Rule 4: eGFR 45-59 AND ACR 3-30
-- - Rule 5: eGFR 30-44 AND ACR < 3 (excludes if rule fails)

with
-- Parent population: Patients on CKD register
ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

-- Rule 1: Exclude patients already in PG1 (HRC) or PG2 (HR)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
),
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg2_hr') }}
),

-- Diabetes register for Rule 3
diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

-- Rule 2: eGFR > 60 AND ACR >= 30 (compound - both required)
-- vs1 = eGFR codes, vs2 = ACR codes
rule_2_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs1") }})
    where result_value > 60
),
rule_2_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs2") }})
    where result_value >= 30
),
rule_2_combined as (
    select e.person_id
    from rule_2_egfr e
    inner join rule_2_acr a on e.person_id = a.person_id
),

-- Rule 3: On Diabetes Register AND eGFR > 60 AND ACR >= 3 (triple compound)
rule_3_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs1") }})
    where result_value > 60
),
rule_3_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs2") }})
    where result_value >= 3
),
rule_3_combined as (
    select e.person_id
    from rule_3_egfr e
    inner join rule_3_acr a on e.person_id = a.person_id
    inner join diabetes_register dr on e.person_id = dr.person_id
),

-- Rule 4: eGFR 45-59 AND ACR 3-30 (compound - both required)
rule_4_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs1") }})
    where result_value >= 45 and result_value <= 59
),
rule_4_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs2") }})
    where result_value >= 3 and result_value <= 30
),
rule_4_combined as (
    select e.person_id
    from rule_4_egfr e
    inner join rule_4_acr a on e.person_id = a.person_id
),

-- Rule 5: eGFR 30-44 AND ACR < 3 (compound - excludes if fails)
rule_5_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs1") }})
    where result_value >= 30 and result_value <= 44
),
rule_5_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs2") }})
    where result_value < 3
),
rule_5_combined as (
    select e.person_id
    from rule_5_egfr e
    inner join rule_5_acr a on e.person_id = a.person_id
),

-- Combine rule results for all CKD register patients (excluding PG1 and PG2)
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_egfr_acr_high,
        (r3.person_id is not null) as rule_3_diabetes_egfr_acr,
        (r4.person_id is not null) as rule_4_egfr_acr_mid,
        (r5.person_id is not null) as rule_5_egfr_acr_low,
        case
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from ckd_register cr
    left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
    left join pg2_exclusions pg2 on cr.person_id = pg2.person_id
    left join rule_2_combined r2 on cr.person_id = r2.person_id
    left join rule_3_combined r3 on cr.person_id = r3.person_id
    left join rule_4_combined r4 on cr.person_id = r4.person_id
    left join rule_5_combined r5 on cr.person_id = r5.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
      and pg2.person_id is null  -- Exclude PG2 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'CKD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
