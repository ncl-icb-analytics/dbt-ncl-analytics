
-- LTC LCS DBT extract
--
--  AF - MR - Rule 10
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with 


AF_reg as (
    select distinct DR.person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER DR
    )
,
-- Rules 10-14: On anticoagulants with various gender-specific conditions 
--(Male with value ≥1, Female with value ≥2) and age >65 with readings before 2 years

-- Rule 10

   on_af_reg_pg3_mr_vs1 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs1'") }} 
     )
,
   on_af_reg_pg3_mr_vs2 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs2'") }} 
     )
     ,
   on_af_reg_pg3_mr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs3'") }} 
     )
     ,
   on_af_reg_pg3_mr_vs4 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs4'") }} 
     )

select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs1 VS1
on DR.person_id = VS1.person_id
left join on_af_reg_pg3_mr_vs2 VS2
on DR.person_id = VS2.person_id
left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id
left join on_af_reg_pg3_mr_vs4 VS4
on DR.person_id = VS4.person_id


where
--On anticoagulants in last 6 months
(VS1.person_id is not null
and
  DATEDIFF(day, VS1.order_date, CURRENT_DATE())<=182.6
)
or
(VS2.person_id is not null
and
  DATEDIFF(day, VS2.order_date, CURRENT_DATE())<=182.6
)
or
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)
or
(VS4.person_id is not null
and
  DATEDIFF(day, VS4.order_date, CURRENT_DATE())<=182.6
)