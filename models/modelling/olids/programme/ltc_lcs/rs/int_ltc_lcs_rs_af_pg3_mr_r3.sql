
-- LTC LCS DBT extract
--
--  AF - MR - Rule 3
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

   on_af_reg_pg3_mr_vs1 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs1'") }} 
     )
,
-- EDDIE TO CONFIRM  WHY EMPTY - NEEDS TO BE OBSERVATIONS???


   on_af_reg_pg3_mr_vs11 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs11'") }} 
     )
select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs1 VS1
on DR.person_id = VS1.person_id
left join on_af_reg_pg3_mr_vs11 VS11
on DR.person_id = VS11.person_id
where
--On anticoagulants in last 6 months
(VS1.person_id is not null
and
  DATEDIFF(day, VS1.order_date, CURRENT_DATE())<=182.6
)

and
--with serum creatinine overdue by >15 months
(VS11.person_id is not null
and
  DATEDIFF(day, VS11.order_date, CURRENT_DATE())>456.5 -- 15 months
)