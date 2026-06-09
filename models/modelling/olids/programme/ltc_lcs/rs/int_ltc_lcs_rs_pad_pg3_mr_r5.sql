
-- LTC LCS DBT extract
--
--  PAD - MR - Rule 5
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    
-- Rule 5: Any statin medication in last 12 months OR statin clinical code in last 12 months 
-- OR statin declined in last 1 year



  on_pad_reg_pg3_mr_vs6 as (
       {{ get_ltc_lcs_medication_orders_latest ("'on_pad_reg_pg3_mr_vs6'") }}
    )
  ,
    on_pad_reg_pg3_mr_vs7 as (
       {{ get_ltc_lcs_medication_orders_latest ("'on_pad_reg_pg3_mr_vs7'") }}
    )
--

select 
 distinct DR.PERSON_ID
from pad_reg DR
left join  on_pad_reg_pg3_mr_vs6 VS6
on DR.PERSON_ID = VS6.PERSON_ID
left join  on_pad_reg_pg3_mr_vs7 VS7
on DR.PERSON_ID = VS7.PERSON_ID

where     DATEDIFF(day, VS6.order_date, CURRENT_DATE())<=365.25
or        DATEDIFF(day, VS7.order_date, CURRENT_DATE())<=365.25
 
