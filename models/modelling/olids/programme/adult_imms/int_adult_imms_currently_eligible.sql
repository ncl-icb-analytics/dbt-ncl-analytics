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
p.TURN_80_AFTER_SEP_2024,
p.TURN_65_AFTER_SEP_2023,
sched.VACCINE_ORDER,
sched.VACCINE_ID,
sched.VACCINE_NAME,
sched.DOSE_NUMBER,
sched.ELIGIBLE_AGE_FROM_DAYS,
sched.ELIGIBLE_AGE_TO_DAYS,
sched.MAXIMUM_AGE_DAYS,
CASE 
--hard coded dates for new adult immunisation programmes RSV and Shingles
WHEN sched.VACCINE_ID = 'RSV_1B' THEN '2024-09-01'
WHEN sched.VACCINE_ID = 'SHING_1' THEN '2023-09-01'
--SHING_2 date hard coded to March 2024 eligibility for those who had SHING_1 in Sept 2023 (6 months later)
WHEN sched.VACCINE_ID = 'SHING_2' THEN '2024-03-01'
ELSE DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_FROM_DAYS)) END AS ELIGIBLE_FROM_DATE,
CASE 
WHEN sched.VACCINE_ID = 'RSV_1B' THEN '2026-08-31'
ELSE DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_TO_DAYS)) END AS ELIGIBLE_TO_DATE,
CASE 
WHEN p.TURN_80_AFTER_SEP_2024 = 'YES' AND sched.VACCINE_ID = 'RSV_1B' AND CURRENT_DATE > '2024-09-01' AND CURRENT_DATE < '2026-03-31' THEN 'Yes' 
WHEN p.TURN_65_AFTER_SEP_2023 = 'YES' AND sched.VACCINE_ID = 'SHING_1' AND CURRENT_DATE > '2023-09-01' AND CURRENT_DATE < DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_TO_DAYS)) THEN 'Yes' 
WHEN p.TURN_80_AFTER_SEP_2024 = 'NO' AND sched.VACCINE_ID = 'RSV_1B' AND CURRENT_DATE > '2024-09-01' AND CURRENT_DATE < '2026-03-31' THEN 'No'
WHEN AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN 'Yes' 
ELSE 'No' END AS CURRENTLY_ELIGIBLE
FROM {{ ref('int_adult_imms_current_population') }} p
CROSS JOIN 
     {{ ref('stg_reference_imms_schedule_adult_latest') }} sched
WHERE 
AGE_DAYS_APPROX >= (select min(ELIGIBLE_AGE_FROM_DAYS) from {{ ref('stg_reference_imms_schedule_adult_latest') }}) 