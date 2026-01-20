-- LTC LCS: Diabetes Register - Priority Group 3 (Moderate Risk)
-- Combines Priority Group 3A (MRa) and Priority Group 3B (MRb)

select 
    person_id,
    'MRa' as subgroup,
    'Included' as final_status
from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }}

union all

select 
    person_id, 
    'MRb' as subgroup, 
    'Included' as final_status
from {{ ref('int_ltc_lcs_rs_dm_pg3b_mrb') }}