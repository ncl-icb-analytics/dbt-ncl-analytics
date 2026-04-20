
-- LTC LCS DBT extract
--
--  AF - HR - Rule 5
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  
,
------Rule 5: Cockcroft-Gault creatinine clearance <40 OR body weight <50kg 
---- OR Rockwood Frailty level 7-9 (severely frail to terminally ill) OR Frailty score ≥7




-- Cockcroft-Gault creatinine clearance
   on_af_reg_pg2_hr_vs10 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs10'") }} 
     )
,
-- body weight
   on_af_reg_pg2_hr_vs11 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs11'") }} 
     )
,
-- Rockwood Frailty level
   on_af_reg_pg2_hr_vs12 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs12'") }} 
     )
,
-- Frailty score
   on_af_reg_pg2_hr_vs13 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs13'") }} 
     )

select DR.person_id from AF_reg DR
left join on_af_reg_pg2_hr_vs10 VS10
on DR.person_id = VS10.person_id
left join on_af_reg_pg2_hr_vs11 VS11
on DR.person_id = VS11.person_id
left join on_af_reg_pg2_hr_vs12 VS12
on DR.person_id = VS12.person_id
left join on_af_reg_pg2_hr_vs13 VS13
on DR.person_id = VS13.person_id
where
------ Cockcroft-Gault creatinine clearance <40


(VS10.person_id is not null
and VS10.result_value <40
)
-- OR body weight <50kg 
or
(VS11.person_id is not null
and VS11.result_value <50
)
-- ---- OR Rockwood Frailty level 7-9
or
(VS12.person_id is not null
and VS12.result_value >= 7
and VS12.result_value <= 9
)
 -- OR Frailty score ≥7
or
(VS13.person_id is not null
and VS13.result_value >= 7
)