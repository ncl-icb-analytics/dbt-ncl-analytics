{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}
--TESTING JESS'S EVALUATION MODEL CREATE FILE FOR PROPENSITY MATCHING Py Script.
--FIND ALL PATIENTS ELIGIBLE FOR EVALUATION USING LATEST ENROLLED FILE
--ELIGIBLE FILES ARE ONLY FROM THE HIGH RISK COHORT - NOT THE WHOLE POPULATION.
with eligibility_files AS ( 
--collect eligible patients who were in all files that have been submitted to RFL so far
--no results for January
    select *, DATE('22-Jan-2026') as file_date
    --from modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }}
    where date('2026-01-22') between dbt_valid_from and coalesce(dbt_valid_to,current_date())
    union 
    --feb extract n=5015
    select *, date('2026-02-16') as file_date
    --from modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }}
    where date('2026-02-16') between dbt_valid_from and coalesce(dbt_valid_to,current_date())
    union 
    --mar extract n=5257
    select *, date('2026-03-17') as file_date
    --from modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }}
    where date('2026-03-17') between dbt_valid_from and coalesce(dbt_valid_to,current_date())
    union 
      --apr extract n=5638
    select *, date('2026-04-15') as file_date
    --from modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }}
    where date('2026-04-15') between dbt_valid_from and coalesce(dbt_valid_to,current_date())
    union 
    --may extract n=5710
    select *, date('2026-05-15') as file_date
    --from modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }}
    where date('2026-05-15') between dbt_valid_from and coalesce(dbt_valid_to,current_date())
    -- TO DO: Add future files pulls as needed
)
--n=6061 - add HEX manually (don't use OLIDS in case patients are missing from there). Use macro in DBT for JEX_ID
, eligibility_information as (
    select 
        patient_id, 
        {{ hxflake_pseudo_generation('patient_id') }} AS hex_id, --THE BELOW SCRIPT IS Safer and production-friendly but the result is the same.
--               CASE
--   WHEN TRY_TO_NUMBER(PATIENT_ID) IS NULL THEN NULL
--   ELSE
--     CONCAT(
--       SUBSTR(
--         RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
--         1, 2
--       ),
--       '-',
--       SUBSTR(
--         RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
--         3, 3
--       ),
--       '-',
--       SUBSTR(
--         RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
--         6, 3
--       )
--     )
-- END AS hex_id,
        file_date as first_file_date,
        local_authority,
        date(most_recent_nel_admission_date) as most_recent_nel_admission_date,
        datediff('day', most_recent_nel_admission_date, file_date) days_since_nel_admission,
        nel_ip_admissions_last_24_months,
        nel_ip_admissions_last_12_months,
        barnet_hospital_flag,
        barnet_hospital_count,
        rfl_flag,
        rfl_count,
        total_high_risk_conditions,
        alcohol_dependence,
        atrial_fibrillation,
        bronchiectasis,
        cerebrovascular_disease,
        chronic_kidney_disease,
        chronic_liver_disease,
        copd,
        coronary_heart_disease,
        dementia,
        end_stage_renal_failure,
        frailty_falls,
        heart_failure,
        hypertension,
        liver_failure,
        osteoporosis,
        parkinsons_disease,
        peripheral_vascular_disease,
        pulmonary_heart_disease,
        rheumatoid_arthritis,
        severe_interstitial_lung_disease
    from 
        eligibility_files 
       --select earliest appearance of person in the snapshots
    qualify row_number() over(partition by patient_id order by file_date) = 1
)
--There are 6061 patients in this group 
--USING OLIDS match HISTORICAL DEMOGRAPHICS RATHER THAN CURRENT 
, demographic_information as (
        select
        e.*,
        --check against PDS as well.
        case when pds.sk_patient_id is not null then 1 else 0 end as on_pds_demog,
        case when pds.sk_patient_id is not null and p.sk_patient_id is not null then 1 else 0 end as is_match_olids,
        case when p.is_active THEN 1 else 0 end as is_active ,
        p.inactive_reason,
         case when p.is_deceased THEN 1 else 0 end as is_deceased,
        p.death_date_approx as death_date,
        p.gender, -- expected: 'male', 'female' or 'unknown'
        --Calculating age by: Taking the year difference, Subtracting 1 if the birthday hasn’t occurred yet this year- expected: 0-100 or null 
        datediff(year, p.birth_date_approx, current_date) -
        case 
        when date_part(dayofyear, p.birth_date_approx) > date_part(dayofyear, current_date) then 1 else 0 end as age_years,
       -- p.age as age_years, --compare
        p.IMD_DECILE_25 as imd_decile,
        case when dc.person_id is not null then 1 else 0 end as is_care_home,
        p.ethnicity_category as ethnicity_group 
    FROM eligibility_information e
    LEFT JOIN {{ ref('dim_person_demographics_basic') }} pds on e.patient_id = pds.sk_patient_id
    --LEFT JOIN REPORTING.COMMISSIONING_REPORTING.DIM_PERSON_DEMOGRAPHICS_BASIC pds on e.patient_id = pds.sk_patient_id
    LEFT JOIN {{ ref('dim_person_demographics_historical') }} p on e.patient_id = p.sk_patient_id
    --LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL p on e.patient_id = p.sk_patient_id
    LEFT JOIN {{ ref('dim_person_care_home') }} dc on p.person_id = dc.person_id
    --LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_CARE_HOME dc on p.person_id = dc.person_id
    --include all historical OLIDS records as latest period_sequence - ie latest but if there is no match to OLIDS then keep in but flag
    QUALIFY P.SK_PATIENT_ID IS NULL OR ROW_NUMBER() OVER (PARTITION BY P.SK_PATIENT_ID ORDER BY PERIOD_SEQUENCE DESC) = 1  
   )
--All patients output - includes patients that did not match OLIDS data (is_match_olids = 0). This is because of the IS_CONFIDENTIAL flag in EMIS.
select 
    -- Eligibility fields
    --need patient id for matching to UEC and APC Encounter tables
    d.patient_id,
    d.hex_id,
    d.on_pds_demog,
    d.is_match_olids,
    d.is_active,
    d.inactive_reason,
    d.death_date,
    d.first_file_date,
    d.local_authority,
    -- Myria status fields
    case when m.enrolled_date is not null then 1 else 0 end as enrolled,
    enrolled_date,
    case when m.onboarded_date is not null then 1 else 0 end as onboarded,
    onboarded_date,
    datediff('day', onboarded_date, current_date) as days_onboarded,
    case when m.discharged_date is not null then 1 else 0 end as discharged,
    discharged_date,
    -- Demographic fields
    d.gender,
    d.age_years,
    d.imd_decile,
    d.is_care_home,
    d.ethnicity_group,
    -- High risk conditions fields
    d.most_recent_nel_admission_date,
    d.days_since_nel_admission,
    d.nel_ip_admissions_last_24_months as nel_admissions_2_years,
   -- d.nel_ip_admissions_last_12_months,
    d.barnet_hospital_count,
    d.rfl_count,
    d.total_high_risk_conditions,
    d.alcohol_dependence,
    d.atrial_fibrillation,
    d.bronchiectasis,
    d.cerebrovascular_disease,
    d.chronic_kidney_disease,
    d.chronic_liver_disease,
    d.copd,
    d.coronary_heart_disease,
    d.dementia,
    d.end_stage_renal_failure,
    d.frailty_falls,
    d.heart_failure,
    d.hypertension,
    d.liver_failure,
    d.osteoporosis,
    d.parkinsons_disease,
    d.peripheral_vascular_disease,
    d.pulmonary_heart_disease,
    d.rheumatoid_arthritis,
    d.severe_interstitial_lung_disease
from demographic_information d
-- Enrolled status from the canonical Myria table, pinned to the 2026-06-03 Doccla file
-- (was raw_reference_myria_enrolled_patients_20260603). One file_date per join to avoid
-- fan-out; switch to the latest file_date once confirmed with Kate.
LEFT JOIN (
    select * from {{ ref('stg_myria_enrolled_patients') }} where file_date = '2026-06-03'
) m ON d.hex_id = m.hx_id

