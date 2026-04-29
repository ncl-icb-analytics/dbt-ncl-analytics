-- LTC LCS DBT extract
--
--  Stroke - HR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--





with stroketia_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_STROKE_TIA_REGISTER
    )  
,

bp as (
    select row_number() over (partition by person_id order by clinical_effective_date desc) as row_no,
    * from {{ ref('int_blood_pressure_all') }}

)


 select distinct DR.person_id from stroketia_reg DR
 inner join bp BP
 on DR.PERSON_ID = BP.PERSON_ID
 where BP.row_no<=100
 and DATEDIFF(day, BP.clinical_effective_date, CURRENT_DATE())<365.25
 and (
    BP.DIASTOLIC_VALUE <= 90
    and
    BP.SYSTOLIC_VALUE <= 140
 )