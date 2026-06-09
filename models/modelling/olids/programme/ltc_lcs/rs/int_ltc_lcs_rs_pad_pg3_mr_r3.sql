
-- LTC LCS DBT extract
--
--  PAD - MR - Rule 2
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    
--Rule 3: Non-HDL cholesterol >2.5



  on_pad_reg_pg3_mr_vs2 as (
       {{ get_ltc_lcs_observations("'on_pad_reg_pg3_mr_vs2'") }}
    )
  
select distinct DR.PERSON_ID
from pad_reg DR
inner join  on_pad_reg_pg3_mr_vs2 VS2
on DR.PERSON_ID = VS2.PERSON_ID

 where VS2.result_value > 2.5
 
