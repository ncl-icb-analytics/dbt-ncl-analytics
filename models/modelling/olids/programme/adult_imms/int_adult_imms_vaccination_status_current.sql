--person level vaccination record for adults
{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

with vacc_status_adult as (
SELECT
  cv.person_id
  ,MAX(CASE WHEN vaccine_id = 'PNEUMO_1' THEN cv.EVENT_TYPE END) as pneumo_status_dose_1
   ,MAX(CASE WHEN vaccine_id = 'PNEUMO_1' THEN cv.EVENT_DATE END) as pneumo_date_dose_1
   ,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.EVENT_TYPE END) as shing_status_dose_1
  ,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.EVENT_DATE END) as shing_date_dose_1
  ,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.EVENT_TYPE END) as shing_status_dose_2
  ,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B') THEN cv.EVENT_DATE END) as shing_date_dose_2
  ,MAX(CASE WHEN vaccine_id = 'RSV_1' THEN cv.EVENT_TYPE END) as rsv_status_dose_1
   ,MAX(CASE WHEN vaccine_id = 'RSV_1' THEN cv.EVENT_DATE END) as rsv_date_dose_1
FROM {{ ref('int_adult_imms_vaccination_events_current') }} cv
GROUP BY cv.person_id

)
SELECT
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.GENDER
,p.AGE
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
,CASE
WHEN v.pneumo_status_dose_1 = 'Administration' THEN 'Vaccinated' ELSE pneumo_status_dose_1 END as pneumo_status_dose_1
,v.pneumo_date_dose_1
,CASE
WHEN v.shing_status_dose_1 = 'Administration' THEN 'Vaccinated' ELSE v.shing_status_dose_1 END AS shing_status_dose_1
,v.shing_date_dose_1
,CASE
WHEN v.shing_status_dose_2 = 'Administration' THEN 'Vaccinated' ELSE v.shing_status_dose_2 END AS shing_status_dose_2
,v.shing_date_dose_2
,CASE
WHEN v.rsv_status_dose_1 = 'Administration' THEN 'Vaccinated' ELSE v.rsv_status_dose_1 END AS rsv_status_dose_1
,v.rsv_date_dose_1
FROM vacc_status_adult v
INNER JOIN {{ ref('int_adult_imms_current_population') }} p using (person_id)
