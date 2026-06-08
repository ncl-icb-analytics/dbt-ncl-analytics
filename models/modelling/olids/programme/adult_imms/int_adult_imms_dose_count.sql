{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}


--This table creates dose count and date labels for adult immunisations for adults aged 60+ using a base table
SELECT DISTINCT
v.PERSON_ID 
,v.age
,v.age_band_5y
,v.is_care_home_resident
,v.practice_code
,ppv_dose1_label
,ppv_dose1_fiscal
,ppv_dose1_sort
,shing_dose1_label
,shing_dose1_fiscal
,shing_dose1_sort
,shing_dose2_label
,shing_dose2_fiscal
,shing_dose2_sort
,rsv_dose1_label
,rsv_dose1_fiscal
,rsv_dose1_sort
FROM {{ ref('int_adult_imms_dose_base') }} v
--FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_BASE v
LEFT JOIN {{ ref('int_adult_imms_historical_ppv_dose1') }} p1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_HISTORICAL_PPV_DOSE1 p1 using (PERSON_ID)
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose1') }} s1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_HISTORICAL_SHINGLES_DOSE1 s1 using (PERSON_ID)
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose2') }} s2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_HISTORICAL_SHINGLES_DOSE2 s2 using (PERSON_ID)
LEFT JOIN {{ ref('int_adult_imms_historical_rsv_dose1') }} r1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_HISTORICAL_RSV_DOSE1 r1 using (PERSON_ID)