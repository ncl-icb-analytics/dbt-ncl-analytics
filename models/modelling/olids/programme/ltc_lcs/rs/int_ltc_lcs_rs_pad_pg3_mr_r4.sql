
-- LTC LCS DBT extract
--
--  PAD - MR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    
-- Rule 4: Repeat High-intensity statins (Atorvastatin 80mg, Lipitor 80mg, Crestor 20-40mg, Rosuvastatin 20-40mg) in last 6 months (excludes if passed - on optimal therapy)



  on_pad_reg_pg3_mr_vs3 as (
       {{ get_ltc_lcs_medication_orders_latest ("'on_pad_reg_pg3_mr_vs3'") }}
    )
  ,
    on_pad_reg_pg3_mr_vs4 as (
       {{ get_ltc_lcs_medication_orders_latest ("'on_pad_reg_pg3_mr_vs4'") }}
    )
--

select 
 distinct DR.PERSON_ID
from pad_reg DR
left join  on_pad_reg_pg3_mr_vs3 VS3
on DR.PERSON_ID = VS3.PERSON_ID
left join  on_pad_reg_pg3_mr_vs4 VS4
on DR.PERSON_ID = VS4.PERSON_ID

where     DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
or        DATEDIFF(day, VS4.order_date, CURRENT_DATE())<=182.6
 
-- NEED TO ADD REPEAT MEDS ONLY