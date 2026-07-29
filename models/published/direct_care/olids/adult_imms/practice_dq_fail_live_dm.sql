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
from {{ ref('emis_olids_reg_fail') }} eq   
LEFT join (select distinct BOROUGH_REGISTERED, practice_code, practice_name 
from {{ ref('dim_person_demographics') }}) p using (practice_code)   
where practice_code <> 'Y03103' order by 1, 2