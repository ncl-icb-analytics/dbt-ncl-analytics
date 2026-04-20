

-- LTC LCS DBT extract
--
--  AF - HR - Rule 3
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  
,
--Rule 3: HAS-BLED score ≥3 OR ORBIT-AF bleeding risk score ≥4

-- HAS-BLED score 
   on_af_reg_pg2_hr_vs7 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs7'") }} 
     )
,
 
-- ORBIT-AF bleeding risk score

   on_af_reg_pg2_hr_vs8 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg2_hr_vs8'") }} 
     )



select DR.person_id from AF_reg DR
left join on_af_reg_pg2_hr_vs7 VS7
on DR.person_id = VS7.person_id
left join on_af_reg_pg2_hr_vs8 VS8
on DR.person_id = VS8.person_id
where
(VS7.person_id is not null
and VS7.result_value >= 3
)
or
(VS8.person_id is not null
and VS8.result_value >= 4
)