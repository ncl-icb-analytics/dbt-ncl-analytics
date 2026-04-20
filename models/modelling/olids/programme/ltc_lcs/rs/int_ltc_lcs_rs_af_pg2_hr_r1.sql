-- LTC LCS DBT extract
--
--  Atrial Fibrillation - HR - Rule 1
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  
,
--Rule 1: On oral anticoagulants OR DOACs (Dabigatran, Rivaroxaban, Apixaban, Edoxaban, Acenocoumarol, Phenindione) in last 6 months 
--OR Anticoagulant prescribed by third party code

--oral anticoagulants
   on_af_reg_pg2_hr_vs1 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs1'") }} 
     )
,
-- Dabigatran
   on_af_reg_pg2_hr_vs2 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs2'") }} 
     )
,
-- Apixaban
   on_af_reg_pg2_hr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs3'") }} 
     )
,
-- Anticoagulant prescribed by third party
   on_af_reg_pg2_hr_vs4 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg2_hr_vs4'") }} 
     )
select DR.* from AF_reg DR
left join on_af_reg_pg2_hr_vs1 VS1
on DR.person_id = VS1.person_id
left join on_af_reg_pg2_hr_vs2 VS2
on DR.person_id = VS2.person_id
left join on_af_reg_pg2_hr_vs3 VS3
on DR.person_id = VS3.person_id
left join on_af_reg_pg2_hr_vs4 VS4
on DR.person_id = VS4.person_id

where
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
(
    VS4.person_id is not null
)
