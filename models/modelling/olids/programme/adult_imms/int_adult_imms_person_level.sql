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
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.vaccination_status END) as shing_status_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.vaccination_date END) as shing_date_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.vaccination_status END) as shing_status_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.vaccination_date END) as shing_date_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_2
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B') THEN cv.vaccination_status END) as rsv_status_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B') THEN cv.vaccination_date END) as rsv_date_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B') THEN cv.AGE_AT_EVENT END) as rsv_age_event_dose_1
FROM {{ ref('int_adult_imms_vaccination_status_current') }} cv
GROUP BY cv.person_id

)
SELECT
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.GENDER
,p.AGE
,p.IS_CARE_HOME_RESIDENT
,p.TURN_65_AFTER_SEP_2023
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
,v.shing_age_event_dose_1
,v.shing_status_dose_2 
,v.shing_date_dose_2
,v.shing_age_event_dose_2
,v.rsv_status_dose_1 
,v.rsv_date_dose_1
,v.rsv_age_event_dose_1
FROM vacc_status_adult v
INNER JOIN {{ ref('int_adult_imms_current_population') }} p using (person_id)