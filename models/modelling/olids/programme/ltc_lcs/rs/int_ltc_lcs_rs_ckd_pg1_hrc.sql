-- LTC LCS: CKD Register - Priority Group 1 (High Risk & Complex)
-- Implements inclusion/exclusion rules for high-risk & complex patient identification
-- Parent population: CKD register
--
-- Inclusion rules:
-- - Rule 1: eGFR < 15 (latest value) → Include
-- - Rule 2: ACR > 250 (latest value) → Include (if Rule 1 failed)
-- Logic: Patient is included if eGFR < 15 OR ACR > 250

with
-- Parent population: Patients currently on CKD register
ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

-- Rule 1: eGFR < 15 (inclusion)
-- Codes: GFR (glomerular filtration rate) calculated by abbreviated MDRD, eGFR, etc.
-- vs1 = eGFR codes
rule_1_egfr_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg1_hrc_vs1") }})
    where result_value < 15
),

-- Rule 2: ACR > 250 (inclusion if Rule 1 failed)
-- Codes: Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, etc.
-- vs2 = ACR codes
rule_2_acr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg1_hrc_vs2") }})
    where result_value > 250
),

-- Combine rule results for all CKD register patients
patient_rules as (
    select
        cr.person_id,
        (r1.person_id is not null) as rule_1_egfr_low,
        (r2.person_id is not null) as rule_2_acr_high,
        case
            when r1.person_id is not null then 'Included'  -- Rule 1 passed
            when r2.person_id is not null then 'Included'  -- Rule 2 passed
            else 'Excluded'                                 -- Both rules failed
        end as final_status
    from ckd_register cr
    left join rule_1_egfr_low r1 on cr.person_id = r1.person_id
    left join rule_2_acr_high r2 on cr.person_id = r2.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'CKD' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
