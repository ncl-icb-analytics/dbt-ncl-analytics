{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}
--practice list with failing EMIS registration data quality checks. 
select  p.BOROUGH_REGISTERED as borough 
,eq.practice_code 
,p.practice_name 
,eq.issue_category    
from published_reporting__direct_care.olids_db_utils.emis_olids_reg_fail eq   
LEFT join (select distinct BOROUGH_REGISTERED, practice_code, practice_name from REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS) p using (practice_code)   
where practice_code <> 'Y03103' order by 1, 2