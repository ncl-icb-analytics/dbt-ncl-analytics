
-- LTC LCS DBT extract
--
--  AF - HR - Rule 2
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  
,
--Rule 2: On antiplatelet therapy (Aspirin, Clopidogrel, Prasugrel, Ticagrelor, Dipyridamole) OR aspirin prophylaxis codes in last 6 months


-- On antiplatelet therapy 
   on_af_reg_pg2_hr_vs5 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs5'") }} 
     )
,
-- aspirin prophylaxis codes

   on_af_reg_pg2_hr_vs6 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs6'") }} 
     )


select DR.* from AF_reg DR
left join on_af_reg_pg2_hr_vs5 VS5
on DR.person_id = VS5.person_id
left join on_af_reg_pg2_hr_vs6 VS6
on DR.person_id = VS6.person_id
where
(VS5.person_id is not null
and
  DATEDIFF(day, VS5.order_date, CURRENT_DATE())<=182.6
)
or
(VS6.person_id is not null
and
  DATEDIFF(day, VS6.order_date, CURRENT_DATE())<=182.6
)