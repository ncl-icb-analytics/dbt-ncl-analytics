{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}


with vacc_status_adol as (
SELECT
  cv.person_id
,MAX(CASE WHEN  vaccine_id = '6IN1_1' THEN cv.vaccination_status END) as sixin1_status_dose_1
,MAX(CASE WHEN  vaccine_id = '6IN1_1' THEN cv.vaccination_date END) as sixin1_date_dose_1
,MAX(CASE WHEN vaccine_id = '6IN1_2' THEN cv.vaccination_status END) as sixin1_status_dose_2
,MAX(CASE WHEN vaccine_id = '6IN1_2' THEN cv.vaccination_date END) as sixin1_date_dose_2
,MAX(CASE WHEN vaccine_id = '6IN1_3' THEN cv.vaccination_status END) as sixin1_status_dose_3
,MAX(CASE WHEN vaccine_id = '6IN1_3' THEN cv.vaccination_date END) as sixin1_date_dose_3
,MAX(CASE WHEN vaccine_id = 'HIBMENC_1' THEN cv.vaccination_status END) as hibmc_status_dose_1
,MAX(CASE WHEN vaccine_id = 'HIBMENC_1' THEN cv.vaccination_date END) as hibmc_date_dose_1
,MAX(CASE WHEN vaccine_id = 'MMR_1' THEN cv.vaccination_status END) as mmr_status_dose_1
,MAX(CASE WHEN vaccine_id = 'MMR_1' THEN cv.vaccination_date END) as mmr_date_dose_1
,MAX(CASE WHEN vaccine_id = 'MMR_2' THEN cv.vaccination_status END) as mmr_status_dose_2
,MAX(CASE WHEN vaccine_id = 'MMR_2' THEN cv.vaccination_date END) as mmr_date_dose_2
,MAX(CASE WHEN vaccine_id = '4IN1_1' THEN cv.vaccination_status END) as fourin1_status_dose_1
,MAX(CASE WHEN vaccine_id = '4IN1_1' THEN cv.vaccination_date END) as fourin1_date_dose_1
,MAX(CASE WHEN vaccine_id = 'HPV_1' THEN cv.vaccination_status END) as hpv_status_dose_1
,MAX(CASE WHEN vaccine_id = 'HPV_1' THEN cv.vaccination_date END) as hpv_date_dose_1
,MAX(CASE WHEN vaccine_id = '3IN1_1' THEN cv.vaccination_status END) as threein1_status_dose_1
,MAX(CASE WHEN vaccine_id = '3IN1_1' THEN cv.vaccination_date END) as threein1_date_dose_1
,MAX(CASE WHEN vaccine_id = 'MENACWY_1' THEN cv.vaccination_status END) as menacwy_status_dose_1
,MAX(CASE WHEN vaccine_id = 'MENACWY_1' THEN cv.vaccination_date END) as menacwy_date_dose_1
FROM {{ ref('int_childhood_imms_vaccination_status_current') }} cv
GROUP BY cv.person_id
)
SELECT 
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.AGE
,p.GENDER
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
,p.residential_borough
,p.residential_neighbourhood
,p.RESIDENTIAL_LOC
,p.ward_code
,p.ward_name
,p.lac_flag
,v.sixin1_status_dose_1
,v.sixin1_date_dose_1
,v.sixin1_status_dose_2
,v.sixin1_date_dose_2
,v.sixin1_status_dose_3
,v.sixin1_date_dose_3
,v.hibmc_status_dose_1
,v.hibmc_date_dose_1
,v.mmr_status_dose_1
,v.mmr_date_dose_1
,v.mmr_status_dose_2
,v.mmr_date_dose_2
,v.fourin1_status_dose_1
,v.fourin1_date_dose_1
,v.hpv_status_dose_1
,v.hpv_date_dose_1
,v.threein1_status_dose_1
,v.threein1_date_dose_1
,v.menacwy_status_dose_1
,v.menacwy_date_dose_1
FROM VACC_STATUS_ADOL v
INNER JOIN {{ ref('int_childhood_imms_current_population') }} p using (person_id)
--replace with accurate AGE flag when available
WHERE p.age BETWEEN 11 and 18