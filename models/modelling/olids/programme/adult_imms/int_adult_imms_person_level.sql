{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

with vacc_status_adult as (
SELECT
  cv.person_id
,MAX(CASE WHEN vaccine_id = 'PPV_1' THEN cv.vaccination_status END) as ppv_status_dose_1
,MAX(CASE WHEN vaccine_id = 'PPV_1' THEN cv.vaccination_date END) as ppv_date_dose_1
,MAX(CASE WHEN vaccine_id = 'PPV_1' THEN cv.AGE_AT_EVENT END) as ppv_age_event_dose_1
-- Shingles Dose 1
  ,COALESCE(
    -- Rule 1 for those turned 65 on after 1st sep 2023 prefer SHING_1
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND vaccine_id = 'SHING_1' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND vaccine_id = 'SHING_1B' THEN vaccination_status END),
 -- Rule 2 for those turned 65 BEFORE 1st sep 2023 prefer SHING_1B
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND vaccine_id = 'SHING_1B' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND vaccine_id = 'SHING_1' THEN vaccination_status END)           
) AS shing_status_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.vaccination_date END) as shing_date_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_1
-- Shingles Dose 2
  ,COALESCE(
    -- Rule 1 for those turned 65 on after 1st sep 2023 prefer SHING_1
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND vaccine_id = 'SHING_2' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND vaccine_id = 'SHING_2B' THEN vaccination_status END),
 -- Rule 2 for those turned 65 BEFORE 1st sep 2023 prefer SHING_1B
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND vaccine_id = 'SHING_2B' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND vaccine_id = 'SHING_2' THEN vaccination_status END)           
) AS shing_status_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.vaccination_date END) as shing_date_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_2
-- RSV
  ,COALESCE(
    -- Rule 1 for those AGED 75 or OLDER starting 1st April 2026 prefer RSV_1
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND vaccine_id = 'RSV_1' THEN vaccination_status END),
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND vaccine_id = 'RSV_1B' THEN vaccination_status END),
 -- Rule 2 for those IN A CARE HOME FOR OLDER PEOPLE  prefer RSV_1B
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT AND vaccine_id = 'RSV_1B' THEN vaccination_status END),
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT AND vaccine_id = 'RSV_1' THEN vaccination_status END)           
) AS rsv_status_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B') THEN cv.vaccination_date END) as rsv_date_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B') THEN cv.AGE_AT_EVENT END) as rsv_age_event_dose_1
FROM {{ ref('int_adult_imms_vaccination_status_current') }} cv
GROUP BY cv.person_id

)
SELECT
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.GENDER
,p.BIRTH_DATE_APPROX
,p.AGE
,p.AGE_BAND_5Y
,p.IS_CARE_HOME_RESIDENT
,p.IS_PREGNANT
,p.IS_IMMUNOSUPPRESSED
,p.TURN_65_AFTER_SEP_2023
,p.TURN_75_AFTER_SEP_2024
,p.TURN_80_AFTER_SEP_2024
,p.ethnicity_category
,p.ethcat_order
,p.ethnicity_subcategory
,p.ethsubcat_order
,p.ethnicity_granular 
,p.imd_quintile
,p.imdquintile_order
,p.imd_decile
,p.main_language
,p.practice_borough
,p.practice_neighbourhood
,p.primary_care_network
,p.gp_name
,p.practice_code
,p.RESIDENTIAL_BOROUGH
,p.residential_neighbourhood
,p.RESIDENTIAL_LOC
,p.ward_code
,p.ward_name
,p.lsoa_code_21
,v.ppv_status_dose_1 
,v.ppv_date_dose_1
,v.ppv_age_event_dose_1
,v.shing_status_dose_1 
,v.shing_date_dose_1
,v.shing_age_event_dose_1
,v.shing_status_dose_2 
,v.shing_date_dose_2
,v.shing_age_event_dose_2
,v.rsv_status_dose_1 
,v.rsv_date_dose_1
,v.rsv_age_event_dose_1
FROM vacc_status_adult v
INNER JOIN {{ ref('int_adult_imms_current_population') }} p using (person_id)