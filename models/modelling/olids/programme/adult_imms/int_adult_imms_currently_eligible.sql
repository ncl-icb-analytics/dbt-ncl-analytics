{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}


SELECT 
p.PERSON_ID,
p.BIRTH_DATE_APPROX,
p.AGE,
p.AGE_DAYS_APPROX,
sched.VACCINE_ORDER,
sched.VACCINE_ID,
sched.VACCINE_NAME,
sched.DOSE_NUMBER,
sched.ELIGIBLE_AGE_FROM_DAYS,
sched.ELIGIBLE_AGE_TO_DAYS,
sched.MAXIMUM_AGE_DAYS,
DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_FROM_DAYS)) AS ELIGIBLE_FROM_DATE,
DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_TO_DAYS)) AS ELIGIBLE_TO_DATE,
CASE 
        WHEN AGE_DAYS_APPROX >= sched.eligible_age_from_days
             AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN 'Yes' 
        ELSE 'No' 
    END AS CURRENTLY_ELIGIBLE
FROM {{ ref('int_adult_imms_current_population') }} p
CROSS JOIN 
     {{ ref('stg_reference_imms_schedule_adult_latest') }} sched
WHERE 
AGE_DAYS_APPROX >= (select min(ELIGIBLE_AGE_FROM_DAYS) from {{ ref('stg_reference_imms_schedule_adult_latest') }}) 