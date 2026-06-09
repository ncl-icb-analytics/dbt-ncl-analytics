-- LTC LCS DBT extract
--
--  NAFLD - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with 
on_nafld_reg_v2_vs1 as (
        {{ get_ltc_lcs_observations("'nafld_reg_v2_vs1'") }}
)

/* 
-- Rule vs2 not included as MASLD codes are subset of vs1 codes
,
 on_nafld_reg_v2_vs2 as (
    
    {{ get_observations("'MASLD_DX_CODES'") }}

    )
*/
     select distinct person_id
      from 
    on_nafld_reg_v2_vs1
 /* 
   union 
 
    select distinct person_id
     -- 'VS2' as flag    
     from 
    on_nafld_reg_v2_vs2
*/
