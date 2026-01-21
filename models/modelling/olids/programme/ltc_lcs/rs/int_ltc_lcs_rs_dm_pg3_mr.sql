-- LTC LCS: Diabetes Register - Priority Group 3 (Moderate Risk)
-- Combines Priority Group 3A (MRa) and Priority Group 3B (MRb)

select
    person_id,
    final_status,
    condition,
    priority_group,
    risk_group
from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }}

union all

select
    person_id,
    final_status,
    condition,
    priority_group,
    risk_group
from {{ ref('int_ltc_lcs_rs_dm_pg3b_mrb') }}