{{
    config(
        materialized='view')
}}


/*
Inpatient encounters from SUS

Clinical Purpose:
- Establishing use of inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as encounter_id
    , core.sk_patient_id
    , core.spell_care_location_site_code_of_treatment as site_id
    , core.spell_admission_date as start_date
    , core.spell_discharge_length_of_hospital_stay as duration
    , core.spell_commissioning_grouping_core_hrg as acuity_proxy
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis  || ', ' || spell_clinical_coding_grouper_derived_secondary_diagnosis  as flat_diagnosis_codes
    , spell_clinical_coding_grouper_derived_dominant_procedure as primary_treatment
    , 'SUS_APC' as source
    -- TO DO: add pod and pod_group
    , IFF(spell_admission_admission_sub_type = 'NON', spell_admission_admission_type, spell_admission_admission_sub_type) as type
    , core.spell_commissioning_tariff_calculation_final_price as cost

from {{ ref('stg_sus_apc_spell')}} as core
