{{
    config(
        materialized='view')
}}


/*
Recent inpatient activities from SUS

Clinical Purpose:
- Establishing use of inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

{% set years_from_now = -1 %}

select 
    /* Information needed to derive standard event information */
    core.primarykey_id as event_id
    , core.spell_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , core.spell_care_location_site_code_of_treatment as site_id
    , core.spell_admission_date as start_date
    , core.spell_discharge_length_of_hospital_stay as duration
    , core.spell_commissioning_grouping_core_hrg as acuity_proxy
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis  || ', ' || spell_clinical_coding_grouper_derived_secondary_diagnosis  as flat_diagnosis_codes
    , spell_clinical_coding_grouper_derived_dominant_procedure as primary_treatment
    , 'SUS_APC' as source
    , IFF(spell_admission_admission_sub_type = 'NON', spell_admission_admission_type, spell_admission_admission_sub_type) as type
    , core.spell_commissioning_tariff_calculation_final_price as cost

from {{ ref('stg_sus_apc_spell')}} as core
where spell_admission_date between dateadd(year, {{years_from_now}}, current_date()) and current_date()