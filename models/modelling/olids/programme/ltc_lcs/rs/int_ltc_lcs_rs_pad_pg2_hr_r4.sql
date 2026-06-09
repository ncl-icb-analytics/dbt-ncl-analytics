-- LTC LCS DBT extract
--
--  PAD - HR - Rule 4


-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
-- Rule 4: BP monitoring codes (Refset) in last 12 months, then for same date:
--Must have: Systolic BP >0, latest 100 readings
--Then same date: Diastolic BP >0, latest 100 readings (Refset code)
--Then same date: Diastolic BP >0, latest (check value 1-90)
--Then same date: Systolic BP >0, latest (check value 1-140)
--(Excludes if passed)


with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    
--Must have: Systolic BP >0, latest 100 readings

bp as (
    select row_number() over (partition by person_id order by clinical_effective_date desc) as row_no,
    * from {{ ref('int_blood_pressure_all') }}

)
,
bp_top_100 as (
 select * from  BP
 where BP.row_no<=100
)
,
bp_latest as (
select row_number() over (partition by person_id order by clinical_effective_date desc) as row_no_latest,
    * from bp_top_100 BP
    where BP.SYSTOLIC_VALUE > 0 
qualify row_number() over (partition by bp.person_id  order by bp.clinical_effective_date desc, systolic_observation_id desc) = 1
)



--Then same date: Diastolic BP >0, latest 100 readings (Refset code)
--Then same date: Diastolic BP >0, latest (check value 1-90)
--Then same date: Systolic BP >0, latest (check value 1-140)
--(Excludes if passed)


-- OUTPUT:
 select distinct DR.PERSON_ID from pad_reg DR
 join  bp_latest BP
on DR.PERSON_ID = BP.PERSON_ID
where 
  BP.DIASTOLIC_VALUE <= 90    and
 BP.SYSTOLIC_VALUE <= 140

