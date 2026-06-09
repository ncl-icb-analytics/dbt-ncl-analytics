
-- LTC LCS DBT extract
--
--  AF - HR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  
,
----Rule 4: On DOACs in last 6 months

-- HAS-BLED score 
   on_af_reg_pg2_hr_vs9 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs9'") }} 
     )


select DR.person_id from AF_reg DR
left join on_af_reg_pg2_hr_vs9 VS9
on DR.person_id = VS9.person_id
where
(VS9.person_id is not null
and
  DATEDIFF(day, VS9.order_date, CURRENT_DATE())<=182.6
)