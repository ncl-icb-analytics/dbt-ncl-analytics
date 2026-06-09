
-- LTC LCS DBT extract
--
--  AF - MR - Rule 4
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
-- Rules 3-4: On oral anticoagulants with serum creatinine overdue by >15 months

-- apixaban
   on_af_reg_pg3_mr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs3'") }} 
     )
,
-- EDDIE TO ADVISE AS RETURNING NO RESULTS
   on_af_reg_pg3_mr_vs11 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs11'") }} 
     )
select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id
left join on_af_reg_pg3_mr_vs11 VS11
on DR.person_id = VS11.person_id
where
--On anticoagulants in last 6 months
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)

and
--with serum creatinine overdue by >15 months
(VS11.person_id is not null
and
  DATEDIFF(day, VS11.clinical_effective_date, CURRENT_DATE())>456.5 -- 15 months
)