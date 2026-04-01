-- LTC LCS: CKD Register - Priority Group 3 (Moderate Risk)
-- Parent population: CKD register, excluding PG1 (HRC) and PG2 (HR)
--
-- EMIS final include path is:
-- - eGFR 30-44
-- - ACR < 3
-- - not already in PG1 or PG2

with ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
),

pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg2_hr') }}
),

rule_1_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs1") }})
    where result_value >= 30 and result_value <= 44
),

rule_2_acr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg3_mr_vs2") }})
    where result_value < 3
)

select
    cr.person_id,
    'Included' as final_status,
    'CKD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from ckd_register cr
left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
left join pg2_exclusions pg2 on cr.person_id = pg2.person_id
inner join rule_1_egfr_range r1 on cr.person_id = r1.person_id
inner join rule_2_acr_range r2 on cr.person_id = r2.person_id
where pg1.person_id is null
  and pg2.person_id is null
