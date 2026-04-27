-- LTC LCS: Hypertension Register - Priority Group 4 (Low Risk)
-- Parent population: Hypertension register, excluding PG1, PG2, PG3A, PG3B
--
-- All remaining patients on the hypertension register not assigned to a higher group.

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Exclude all higher priority groups
higher_pg_exclusions as (
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg3a_mra') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg3b_mrb') }}
)

-- Final result: everyone not in a higher group
select
    hr.person_id,
    'Included' as final_status,
    'Hypertension' as condition,
    '4' as priority_group,
    'LR' as risk_group
from hypertension_register hr
left join higher_pg_exclusions excl on hr.person_id = excl.person_id
where excl.person_id is null
