{{
    config(
        materialized='view',
        tags=['myria_eval'])
}}
---CHECKING FOR INACTIVE, DECEASED and PALLIATIVE CARE PATIENTS in COHORT OF ELIGIBLE PATIENTS SENT TO RFL ROUGHLY EVERY 2nd TUESDAY OF THE MONTH
--All patients output - includes patients that did not match OLIDS data (is_match_olids = 0). This is because of the IS_CONFIDENTIAL flag in EMIS.
select
--add HEX
 --{{ hxflake_pseudo_generation('s.patient_id') }} AS hex_id, --THE BELOW SCRIPT IS Safer and production-friendly but the result is the same.
CASE
  WHEN TRY_TO_NUMBER(s.PATIENT_ID) IS NULL THEN NULL
  ELSE
    CONCAT(
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        1, 2
      ),
      '-',
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        3, 3
      ),
      '-',
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        6, 3
      )
    )
END AS HEX_ID,
--PATIENT_ID,
HOSPITAL_NUMBER,
TO_VARCHAR(
    TO_TIMESTAMP_NTZ(TO_VARCHAR(DATA_SOURCE_REFRESH_DATE)),
    'YYYY-MM-DD HH24:MI'
) AS DATA_SOURCE_REFRESH_DATE,
--items from OLIDS historical demographics table
CASE WHEN p.is_active THEN 1 ELSE 0 END AS is_active,
p.borough_registered as NCL_borough_registered,
NVL(p.borough_resident,'Unknown') as borough_resident,
CASE WHEN p.is_deceased THEN 1 ELSE 0 END AS is_deceased,
p.DEATH_DATE_APPROX as death_date_approx,
--cross check with PDS demographics
case when pds.flag_current_ncl_registered THEN 1 else 0 end as is_on_pds,
case when pds.sk_patient_id is not null and p.sk_patient_id is not null then 1 else 0 end as is_match_olids,
case when pds.sk_patient_id is not null and p.sk_patient_id is null then 'Not in OLIDS' else p.inactive_reason end as inactive_reason,
--palliative care register check
case when pall.person_id is not null then 1 else 0 end as is_on_palliative_care_register,
---------------------------
GP_CODE,
GP_NAME AS GP_PRACTICE_NAME,
LOCAL_AUTHORITY,
BARNET_HOSPITAL_COUNT AS "BH NEL Admissions (1 Year)",
RFL_COUNT AS "RFL Admissions (1 Year)",
NEL_IP_ADMISSIONS_LAST_24_MONTHS AS "NEL Admissions (2 Years)",
CASE WHEN s.most_recent_nel_discharge_date_bh  >= DATEADD('month', -12, CURRENT_DATE) THEN 1 ELSE 0 END AS BH_DISCHARGE_LAST_12M,
--NEL_IP_ADMISSIONS_LAST_12_MONTHS,
ALCOHOL_DEPENDENCE,
ATRIAL_FIBRILLATION,
BRONCHIECTASIS,
CEREBROVASCULAR_DISEASE,
CHRONIC_KIDNEY_DISEASE,
CHRONIC_LIVER_DISEASE,
COPD,
CORONARY_HEART_DISEASE,
DEMENTIA,
END_STAGE_RENAL_FAILURE,
FRAILTY_FALLS,
HEART_FAILURE,
HYPERTENSION,
LIVER_FAILURE,
OSTEOPOROSIS,
PARKINSONS_DISEASE,
PERIPHERAL_VASCULAR_DISEASE,
PULMONARY_HEART_DISEASE,
RHEUMATOID_ARTHRITIS,
SEVERE_INTERSTITIAL_LUNG_DISEASE,
TOTAL_HIGH_RISK_CONDITIONS,
TO_VARCHAR(
    TO_TIMESTAMP_NTZ(TO_VARCHAR(MOST_RECENT_NEL_ADMISSION_DATE)),
    'YYYY-MM-DD HH24:MI'
) AS MOST_RECENT_NEL_ADMISSION_DATE
--FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT s
FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} s
LEFT JOIN (select person_id, sk_patient_id, BOROUGH_REGISTERED, borough_resident, is_active, inactive_reason, is_deceased, death_date_approx
,ROW_NUMBER() OVER (PARTITION BY SK_patient_id ORDER BY PERIOD_SEQUENCE DESC) AS row_num, 
--FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL
FROM {{ ref('dim_person_demographics_historical') }} QUALIFY row_num = 1) p on s.patient_id = p.sk_patient_id
--additional check with PDS
--LEFT JOIN REPORTING.COMMISSIONING_REPORTING.DIM_PERSON_DEMOGRAPHICS_BASIC pds on s.patient_id = pds.sk_patient_id
LEFT JOIN {{ ref('dim_person_demographics_basic') }} pds on s.patient_id = pds.sk_patient_id
--check for palliative care register
--LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PALLIATIVE_CARE_REGISTER  pall on pall.person_id = p.person_id
LEFT JOIN {{ ref('fct_person_palliative_care_register') }} pall on pall.person_id = p.person_id

WHERE dbt_valid_to is null

ORDER BY BARNET_HOSPITAL_COUNT DESC
 