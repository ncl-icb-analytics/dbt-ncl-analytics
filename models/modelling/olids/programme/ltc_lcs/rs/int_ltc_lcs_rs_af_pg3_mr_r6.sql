

-- LTC LCS DBT extract
--
--  AF - MR - Rule 6
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


--Rule 6: Body Weight
   on_af_reg_pg3_mr_vs6 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs6'") }} 
     )

     --


select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id

left join on_af_reg_pg3_mr_vs6 VS6
on DR.person_id = VS6.person_id

where
--On anticoagulants in last 6 months
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)


and

 -- Rule 6: weight ≥50kg

  (VS6.person_id is not null
  and VS6.result_value > 50
  and VS6.result_value <= 60 -- CS 25/2/26 note this is slightly different range to rule 2
  )

