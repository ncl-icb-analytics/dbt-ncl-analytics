{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

with vacc_status_child as (
SELECT
  cv.person_id
  ,MAX(CASE WHEN  vaccine_order = 1 THEN cv.vaccination_status END) as sixin1_status_dose_1
  ,MAX(CASE WHEN  vaccine_order = 1 THEN cv.vaccination_date END) as sixin1_date_dose_1
  ,MAX(CASE WHEN vaccine_order = 4 THEN cv.vaccination_status END) as sixin1_status_dose_2
  ,MAX(CASE WHEN vaccine_order = 4 THEN cv.vaccination_date END) as sixin1_date_dose_2
  ,MAX(CASE WHEN vaccine_order = 7 THEN cv.vaccination_status END) as sixin1_status_dose_3
  ,MAX(CASE WHEN vaccine_order = 7 THEN cv.vaccination_date END) as sixin1_date_dose_3
  ,MAX(CASE WHEN vaccine_order = 2 THEN cv.vaccination_status END) as menb_status_dose_1
  ,MAX(CASE WHEN vaccine_order = 2 THEN cv.vaccination_date END) as menb_date_dose_1
  ,MAX(CASE WHEN vaccine_order = 8  THEN cv.vaccination_status END) as menb_status_dose_2
  ,MAX(CASE WHEN vaccine_order = 8 THEN cv.vaccination_date END) as menb_date_dose_2
  ,MAX(CASE WHEN vaccine_order = 12 THEN cv.vaccination_status END) as menb_status_dose_3
  ,MAX(CASE WHEN vaccine_order = 12 THEN cv.vaccination_date END) as menb_date_dose_3
  ,MAX(CASE WHEN vaccine_order = 3 THEN cv.vaccination_status END) as rota_status_dose_1
  ,MAX(CASE WHEN vaccine_order = 3 THEN cv.vaccination_date END) as rota_date_dose_1
  ,MAX(CASE WHEN vaccine_order = 6 THEN cv.vaccination_status END) as rota_status_dose_2
	,MAX(CASE WHEN vaccine_order = 6 THEN cv.vaccination_date END) as rota_date_dose_2
  ,MAX(CASE WHEN vaccine_order = 5 THEN cv.vaccination_status END) as pcv_status_dose_1
	,MAX(CASE WHEN vaccine_order = 5 THEN cv.vaccination_date END) as pcv_date_dose_1
  ,MAX(CASE WHEN vaccine_order = 10 THEN cv.vaccination_status END) as pcv_status_dose_2
	,MAX(CASE WHEN vaccine_order = 10 THEN cv.vaccination_date END) as pcv_date_dose_2
	,MAX(CASE WHEN vaccine_order = 9 THEN cv.vaccination_status END) as hibmc_status_dose_1
	,MAX(CASE WHEN vaccine_order = 9 THEN cv.vaccination_date END) as hibmc_date_dose_1
	,MAX(CASE WHEN vaccine_order = 11 THEN cv.vaccination_status END) as mmr_status_dose_1
	,MAX(CASE WHEN vaccine_order = 11 THEN cv.vaccination_date END) as mmr_date_dose_1
	,MAX(CASE WHEN vaccine_order = 15 THEN cv.vaccination_status END) as mmr_status_dose_2
	,MAX(CASE WHEN vaccine_order = 15 THEN cv.vaccination_date END) as mmr_date_dose_2
 	,MAX(CASE WHEN vaccine_order = 14 THEN cv.vaccination_status END) as fourin1_status_dose_1
	,MAX(CASE WHEN vaccine_order = 14 THEN cv.vaccination_date END) as fourin1_date_dose_1
FROM {{ ref('int_childhood_imms_vaccination_status_current') }} cv
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
,p.imd_decile
,p.main_language
,p.practice_borough
,p.practice_neighbourhood
,p.primary_care_network
,p.gp_name
,p.practice_code
,p.RESIDENTIAL_BOROUGH
,p.ward_code
,p.ward_name
,p.lac_flag
,v.sixin1_status_dose_1
,v.sixin1_date_dose_1
,v.sixin1_status_dose_2
,v.sixin1_date_dose_2
,v.sixin1_status_dose_3
,v.sixin1_date_dose_3
,v.menb_status_dose_1
,v.menb_date_dose_1
,v.menb_status_dose_2
,v.menb_date_dose_2
,v.menb_status_dose_3
,v.menb_date_dose_3
,v.rota_status_dose_1
,v.rota_date_dose_1
,v.rota_status_dose_2
,v.rota_date_dose_2
,v.pcv_status_dose_1
,v.pcv_date_dose_1
,v.pcv_status_dose_2
,v.pcv_date_dose_2
,v.hibmc_status_dose_1
,v.hibmc_date_dose_1
,v.mmr_status_dose_1
,v.mmr_date_dose_1
,v.mmr_status_dose_2
,v.mmr_date_dose_2
,v.fourin1_status_dose_1
,v.fourin1_date_dose_1
FROM vacc_status_child v
INNER JOIN {{ ref('int_childhood_imms_current_population') }} p using (person_id)

--replace with accurate AGE flag when available
where p.age <11 