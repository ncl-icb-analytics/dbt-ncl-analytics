-- LTC LCS DBT extract
--
--  PAD - HR - Rule 5


-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
--Rule 5: 24hr BP monitoring codes (Refset) in last 12 months, then for same date:
--Must have: Systolic BP >0, latest
--Then same date: Diastolic BP >0, latest (check value >0)
--Then same date: 24hr BP monitoring (Refset) in last 12 months, latest 100
--Then same date: Systolic BP >0, latest 100
--Then same date: Diastolic BP >0, latest (check value 1-85)
--Then same date: Systolic BP >0, latest (check value 1-135)
-- (Excludes if passed)



with pad_reg as (
    select distinct person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER
    )  
,
    

--24hr BP monitoring codes (Refset) in last 12 months
bp as (
    select row_number() over (partition by person_id order by clinical_effective_date desc) as row_no,
    * from {{ ref('int_blood_pressure_all') }}

)
,

bp_12m as (
 select * from  BP
 where 
 DATEDIFF(day, bp.clinical_effective_date, CURRENT_DATE())<=365.25
)
,
--Must have: Systolic BP >0, latest
--Then same date: Diastolic BP >0, latest (check value >0)
bp_latest as (
select row_number() over (partition by person_id order by clinical_effective_date desc) as row_no_latest,
    * from bp_12m BP
    where BP.SYSTOLIC_VALUE > 0 
    and BP.DIASTOLIC_VALUE > 0 
qualify row_number() over (partition by bp.person_id  order by bp.clinical_effective_date desc, systolic_observation_id desc) = 1
)



-- OUTPUT:
 select distinct DR.PERSON_ID from pad_reg DR
 join  bp_latest BP
on DR.PERSON_ID = BP.PERSON_ID
where 
  BP.DIASTOLIC_VALUE <= 85    and
 BP.SYSTOLIC_VALUE <= 135


