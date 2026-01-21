-- LTC LCS: Diabetes Register - Priority Group 4 (Low Risk)
-- Parent population: Diabetes register
--
-- Logic:
-- - Excludes patients in PG1 (HRC), PG2 (HR), PG3A (MRa), and PG3B (MRb)
-- - All remaining diabetes register patients are Low Risk

select
    person_id,
    'Included' as final_status,
    'Diabetes' as condition,
    '4' as priority_group,
    'LR' as risk_group
from {{ ref('fct_person_diabetes_register') }}
where person_id not in (select person_id from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }})
  and person_id not in (select person_id from {{ ref('int_ltc_lcs_rs_dm_pg2_hr') }})
  and person_id not in (select person_id from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }})
  and person_id not in (select person_id from {{ ref('int_ltc_lcs_rs_dm_pg3b_mrb') }})
