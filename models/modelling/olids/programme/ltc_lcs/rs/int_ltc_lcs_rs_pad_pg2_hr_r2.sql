-- LTC LCS DBT extract
--
--  PAD - HR - Rule 2


-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    
--Rule 2: Peripheral ischaemia codes dated 90-365 days before search date

  on_pad_reg_pg2_hr_vs1 as (
       {{ get_ltc_lcs_observations("'on_pad_reg_pg2_hr_vs1'") }}
    )
  
select distinct DR.PERSON_ID
from pad_reg DR
inner join  on_pad_reg_pg2_hr_vs1 VS1
on DR.PERSON_ID = VS1.PERSON_ID

 where DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())>90 
 and DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=365
