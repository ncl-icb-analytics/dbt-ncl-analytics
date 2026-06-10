-- LTC LCS: NAFLD register
-- EMIS source: 'LTC LCS: NAFLD Register v2*'
-- (docs/emis_specs/ltc_lcs_rs/ltc_lcs_nafld_register_v2.md)
--
-- The EMIS register spans the whole steatotic liver disease family (fatty
-- liver conditions including the alcoholic branch, plus MASLD/MASH codes) -
-- broader than fct_person_nafld_register, which stays on the clinically
-- clean MASLD_DX_CODES cluster.
--
-- Implemented from the EMIS valuesets (nafld_reg_v2_vs1/vs2). Switch to the
-- ECL-managed steatotic liver disease cluster (SLD_DX_CODES:
-- << 197321007 OR << 79720007 OR << 29185008, verified equivalent to the
-- EMIS expansion) via get_observations once it lands in combined_codesets.

with
nafld_codes as (
    select person_id from ({{ get_ltc_lcs_observations("nafld_reg_v2_vs1") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("nafld_reg_v2_vs2") }})
),

active as (
    select person_id from {{ ref('dim_person_active_patients') }}
)

select distinct n.person_id
from nafld_codes n
inner join active a on n.person_id = a.person_id
