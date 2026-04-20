

-- LTC LCS DBT extract
--
--  AF - MR - Rule 9
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
-- Rules 8-9: On DOACs with Rockwood Frailty 1-4 OR latest Frailty score 6-7 in last 6 months



-- apixaban
   on_af_reg_pg3_mr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs3'") }} 
     )
,


--Rule 9 - latest Frailty score 6-7 in last 6 months
   on_af_reg_pg3_mr_vs9 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs9'") }} 
     )

     --

select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id

left join on_af_reg_pg3_mr_vs9 VS9
on DR.person_id = VS9.person_id

where
--On anticoagulants in last 6 months
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)

and

  (VS9.person_id is not null
  and VS9.result_value >= 6
    and VS9.result_value < 7
  )