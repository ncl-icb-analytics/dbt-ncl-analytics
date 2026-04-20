
-- LTC LCS DBT extract
--
--  AF - MR - Rule 5
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
-- Rules 5-7: On DOACs with Cockcroft-Gault 40-60 OR weight 50-60kg OR age ≥75


-- apixaban
   on_af_reg_pg3_mr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs3'") }} 
     )
,

--Rule 5: Cockcroft-Gault 40-60
   on_af_reg_pg3_mr_vs5 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs5'") }} 
     )

     --


select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id
left join on_af_reg_pg3_mr_vs5 VS5
on DR.person_id = VS5.person_id


where
--On anticoagulants in last 6 months
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)

--Rule 5: Cockcroft-Gault 40-60
and

  (VS5.person_id is not null
  and VS5.result_value >= 40
  and VS5.result_value < 60
  )


