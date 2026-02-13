{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}
--Feb 2026 updates. These rules are more complex to reflect new vaccinations intriduced in Jan 2026
with vacc_status_child as (
SELECT
  cv.person_id
  ,MAX(CASE WHEN vaccine_id = '6IN1_1' THEN cv.vaccination_status END) as sixin1_status_dose_1
  ,MAX(CASE WHEN vaccine_id = '6IN1_1' THEN cv.vaccination_date END) as sixin1_date_dose_1
  ,MAX(CASE WHEN vaccine_id = '6IN1_2' THEN cv.vaccination_status END) as sixin1_status_dose_2
  ,MAX(CASE WHEN vaccine_id = '6IN1_2' THEN cv.vaccination_date END) as sixin1_date_dose_2
  ,MAX(CASE WHEN vaccine_id = '6IN1_3' THEN cv.vaccination_status END) as sixin1_status_dose_3
  ,MAX(CASE WHEN vaccine_id = '6IN1_3' THEN cv.vaccination_date END) as sixin1_date_dose_3
  ,MAX(CASE WHEN vaccine_id = '6IN1_4' THEN cv.vaccination_status END) as sixin1_status_dose_4
  ,MAX(CASE WHEN vaccine_id = '6IN1_4' THEN cv.vaccination_date END) as sixin1_date_dose_4
  ,MAX(CASE WHEN vaccine_id = 'MENB_1' THEN cv.vaccination_status END) as menb_status_dose_1
  ,MAX(CASE WHEN vaccine_id = 'MENB_1' THEN cv.vaccination_date END) as menb_date_dose_1
  --new vaccine schedule for MENB _2 for children born on or after 1st July 2024
,COALESCE(
    -- Rule for those born on/after 1 July 2024: prefer MENB_2B
    MAX(CASE
            WHEN birth_date_approx >= DATE '2024-07-01' AND vaccine_id = 'MENB_2B' THEN vaccination_status END),
    MAX(CASE
            WHEN birth_date_approx >= DATE '2024-07-01' AND vaccine_id = 'MENB_2' THEN vaccination_status END),
    -- Rule for those born before 1 July 2024: prefer MENB_2
    MAX(CASE
            WHEN birth_date_approx < DATE '2024-07-01' AND vaccine_id = 'MENB_2' THEN vaccination_status END),
    MAX(CASE
            WHEN birth_date_approx < DATE '2024-07-01' AND vaccine_id = 'MENB_2B' THEN vaccination_status END)
) AS menb_status_dose_2
  ,MAX(CASE WHEN vaccine_id in ('MENB_2','MENB_2B') THEN cv.vaccination_date END) as menb_date_dose_2
  ,MAX(CASE WHEN vaccine_id = 'MENB_3' THEN cv.vaccination_status END) as menb_status_dose_3
  ,MAX(CASE WHEN vaccine_id = 'MENB_3' THEN cv.vaccination_date END) as menb_date_dose_3
  ,MAX(CASE WHEN vaccine_id = 'ROTA_1' THEN cv.vaccination_status END) as rota_status_dose_1
  ,MAX(CASE WHEN vaccine_id = 'ROTA_1' THEN cv.vaccination_date END) as rota_date_dose_1
  ,MAX(CASE WHEN vaccine_id = 'ROTA_2' THEN cv.vaccination_status END) as rota_status_dose_2
  ,MAX(CASE WHEN vaccine_id = 'ROTA_2' THEN cv.vaccination_date END) as rota_date_dose_2
  --new vaccine schedule for PCV_1 for children born on or after 1st July 2024
 ,COALESCE(
    -- Rule for those born on/after 1 July 2024: prefer PCV_1B
    MAX(CASE
            WHEN birth_date_approx >= DATE '2024-07-01' AND vaccine_id = 'PCV_1B' THEN vaccination_status END),
    MAX(CASE
            WHEN birth_date_approx >= DATE '2024-07-01' AND vaccine_id = 'PCV_1' THEN vaccination_status END),
    -- Rule for those born before 1 July 2024: prefer PCV_1
    MAX(CASE
            WHEN birth_date_approx < DATE '2024-07-01' AND vaccine_id = 'PCV_1' THEN vaccination_status END),
    MAX(CASE
            WHEN birth_date_approx < DATE '2024-07-01' AND vaccine_id = 'PCV_1B' THEN vaccination_status END)
) AS pcv_status_dose_1
    ,MAX(CASE WHEN vaccine_id in ('PCV_1','PCV_1B') THEN cv.vaccination_date END) as pcv_date_dose_1
    ,MAX(CASE WHEN vaccine_id = 'PCV_2' THEN cv.vaccination_status END) as pcv_status_dose_2
	,MAX(CASE WHEN vaccine_id = 'PCV_2' THEN cv.vaccination_date END) as pcv_date_dose_2
	,MAX(CASE WHEN vaccine_id = 'HIBMENC_1' THEN cv.vaccination_status END) as hibmc_status_dose_1
	,MAX(CASE WHEN vaccine_id = 'HIBMENC_1' THEN cv.vaccination_date END) as hibmc_date_dose_1
	,MAX(CASE WHEN vaccine_id = 'MMR_1' THEN cv.vaccination_status END) as mmr_status_dose_1
	,MAX(CASE WHEN vaccine_id = 'MMR_1' THEN cv.vaccination_date END) as mmr_date_dose_1
  -- new vaccine schedule from January 2026 to introduce MMRV
  ,COALESCE(
    -- Rule 1 for those born on/after 1 January 2025: prefer MMRV_1 
    MAX(CASE
            WHEN BORN_JAN_2025_FLAG = 'Yes' AND vaccine_id = 'MMRV_1' THEN vaccination_status END),
    MAX(CASE
            WHEN BORN_JAN_2025_FLAG = 'Yes' AND vaccine_id in ('MMRV_1B','MMRV_1C') THEN vaccination_status END),
 -- Rule 2 for those born on/after 1 July 2024: prefer MMRV_1B
    MAX(CASE
            WHEN BORN_JUL_2024_FLAG = 'Yes' AND vaccine_id = 'MMRV_1B' THEN vaccination_status END),
    MAX(CASE
            WHEN BORN_JUL_2024_FLAG = 'Yes' AND vaccine_id in ('MMRV_1','MMRV_1C') THEN vaccination_status END),
    -- Rule 3 for those born on or after 1st September 2022: prefer MMR_1 as first dose but also MMRV_1C in place of MMR_2 for second dose
    MAX(CASE
            WHEN BORN_SEP_2022_FLAG = 'Yes' AND vaccine_id = 'MMRV_1C' THEN vaccination_status END),
    MAX(CASE
            WHEN BORN_SEP_2022_FLAG = 'Yes' AND vaccine_id in ('MMRV_1','MMRV_1B') THEN vaccination_status END),
    --Rule 4 for those born before September 2022 Prefer MMR 1 as first dose 
    MAX(CASE
            WHEN (BORN_JAN_2025_FLAG = 'No' AND BORN_JUL_2024_FLAG = 'No' AND BORN_SEP_2022_FLAG = 'No') AND vaccine_id in ('MMRV_1','MMRV_1B','MMRV_1C') THEN vaccination_status END)
            
) AS mmrv_status_dose_1
	,MAX(CASE WHEN vaccine_id in ('MMRV_1','MMRV_1B','MMRV_1C') THEN cv.vaccination_date END) as mmrv_date_dose_1
   	,MAX(CASE WHEN vaccine_id = 'MMR_2' THEN cv.vaccination_status END) as mmr_status_dose_2
	,MAX(CASE WHEN vaccine_id = 'MMR_2' THEN cv.vaccination_date END) as mmr_date_dose_2
    -- new vaccine schedule from January 2026 to introduce MMRV
    ,COALESCE(
     -- Rule 1 for those born on/after 1 January 2025: prefer MMRV_2
    MAX(CASE
            WHEN BORN_JAN_2025_FLAG = 'Yes' AND vaccine_id = 'MMRV_2' THEN vaccination_status END),
    MAX(CASE
            WHEN BORN_JAN_2025_FLAG = 'Yes' AND vaccine_id in ('MMRV_2B') THEN vaccination_status END),
 -- Rule 2 for those born on/after 1 July 2024: prefer MMRV_2B
    MAX(CASE
            WHEN BORN_JUL_2024_FLAG = 'Yes' AND vaccine_id = 'MMRV_2B' THEN vaccination_status END),
    MAX(CASE
            WHEN BORN_JUL_2024_FLAG = 'Yes' AND vaccine_id in ('MMRV_2') THEN vaccination_status END),
 -- Rule 3 for those born on or after 1st September 2022. MMRV_1C replaces MMR_2 
    MAX(CASE
            WHEN BORN_SEP_2022_FLAG = 'Yes' AND vaccine_id in ('MMRV_2','MMRV_2B') THEN vaccination_status END),
--Rule 4 for those born before September 2022 Prefer MMR2 
    MAX(CASE
            WHEN (BORN_JAN_2025_FLAG = 'No' AND BORN_JUL_2024_FLAG = 'No' AND BORN_SEP_2022_FLAG = 'No') AND vaccine_id in ('MMRV_2','MMRV_2') THEN vaccination_status END)           
   ) AS mmrv_status_dose_2
	,MAX(CASE WHEN vaccine_id in ('MMRV_2','MMRV_2B') THEN cv.vaccination_date END) as mmrv_date_dose_2
 	,MAX(CASE WHEN vaccine_id = '4IN1_1' THEN cv.vaccination_status END) as fourin1_status_dose_1
	,MAX(CASE WHEN vaccine_id = '4IN1_1' THEN cv.vaccination_date END) as fourin1_date_dose_1
FROM {{ ref('int_childhood_imms_vaccination_status_current') }} cv
GROUP BY cv.person_id

)
SELECT
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.GENDER
,p.AGE
,p.BORN_JAN_2025_FLAG
,p.BORN_JUL_2024_FLAG
,p.BORN_SEP_2022_FLAG
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
,p.lac_flag
,p.LSOA_CODE_21
,v.sixin1_status_dose_1
,v.sixin1_date_dose_1
,v.sixin1_status_dose_2
,v.sixin1_date_dose_2
,v.sixin1_status_dose_3
,v.sixin1_date_dose_3
,v.sixin1_status_dose_4
,v.sixin1_date_dose_4
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
,v.mmrv_status_dose_1
,v.mmrv_date_dose_1
,v.mmr_status_dose_2
,v.mmr_date_dose_2
,v.mmrv_status_dose_2
,v.mmrv_date_dose_2
,v.fourin1_status_dose_1
,v.fourin1_date_dose_1
FROM vacc_status_child v
INNER JOIN {{ ref('int_childhood_imms_current_population') }} p using (person_id)

--replace with accurate AGE flag when available
where p.age <11 