
-- LTC LCS DBT extract
--
--  AF - MR - Rule 14
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

with 


  patient_age as (
select distinct person_id, age, gender
from  DEV__REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS
)
,

AF_reg as (
    select distinct DR.person_id, AGE.AGE, AGE.gender
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER DR
    inner join patient_age AGE
    on DR.person_id = AGE.person_id
    )
,
-- Rules 10-14: On anticoagulants with various gender-specific conditions 
--(Male with value ≥1, Female with value ≥2) and age >65 with readings before 2 years

-- Rule 12

-- CHADVASC
   on_af_reg_pg3_mr_vs10 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs10'") }} 
   )


     --

select DR.person_id from AF_reg DR

join on_af_reg_pg3_mr_vs10 VS10
on DR.person_id = VS10.person_id

where 
 DR.age >65

