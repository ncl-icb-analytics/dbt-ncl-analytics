-- LTC LCS DBT extract
--
--  PAD - HR - Rule 6


-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

--Rule 6: Age >80 years (go to next rule if passed, include if failed)

with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER DR
    )  
,  patient_age as (
select distinct person_id, age
from  DEV__REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS
)
 
 --

 select distinct DR.PERSON_ID from pad_reg DR
 join patient_age AGE
 on DR.person_id = AGE.person_id
 where age.AGE > 80


