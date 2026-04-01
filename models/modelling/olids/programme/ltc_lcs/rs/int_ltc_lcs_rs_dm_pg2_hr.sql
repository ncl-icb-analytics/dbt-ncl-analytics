-- LTC LCS: Diabetes Register - Priority Group 2 (High Risk)
-- Parent population: Diabetes register, excluding PG1 (HRC)
--
-- EMIS final include path is driven by ACR > 70 after excluding PG1.

with diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
),

rule_2_acr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg2_hr_vs7") }})
    where result_value > 70
)

select
    dr.person_id,
    'Included' as final_status,
    'Diabetes' as condition,
    '2' as priority_group,
    'HR' as risk_group
from diabetes_register dr
left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
inner join rule_2_acr_high r2 on dr.person_id = r2.person_id
where pg1.person_id is null
