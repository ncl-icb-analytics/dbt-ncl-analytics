{{
    config(
        materialized='view'
    )
}}

select
    visit_occurrence_id
    , sk_patient_id
    , source
    , local_patient_identifier
    , organisation_id
    , organisation_name
    , site_id
    , site_name
    , pod
    , department_type
    , start_date
    , duration
    , end_date
    , chief_complaint_code
    , chief_complaint_desc
    , chief_complaint_ecds_group1
    , is_injury_related
    , acuity
    , primary_diagnosis_code_snomed
    , primary_diagnosis_desc_snomed
    , primary_diagnosis_code_icd10
    , primary_diagnosis_desc_icd10
    , primary_diagnosis_desc_ecds_group1
    , primary_treatment
    , primary_treatment_desc_snomed
    , primary_treatment_desc_ecds_group1
    , primary_investigation
    , primary_investigation_desc_snomed
    , primary_investigation_desc_ecds_group1
    , arrival_mode_code
    , arrival_mode_desc
    , discharge_destination_code
    , discharge_destination_desc
    , main_specialty_code
    , main_specialty_name
    , hrg_code
    , core_hrg_desc
    , core_hrg_chapter
    , core_hrg_chapter_desc
    , cost
    , age_at_event
    , gender_at_event
    , gender_desc_at_event
    , ethnicity_at_event
    , ethnicity_desc_at_event
    , postcode_district_at_event
    , lsoa_11_at_event
    , lad_at_event
    , imd_at_event
    , reg_practice_at_event
    , visit_occurrence_type
from {{ ref('int_sus_uec_encounter') }}
