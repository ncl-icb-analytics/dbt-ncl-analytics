{{
    config(materialized = 'view')
}}

select primarykey_id 
    , spell_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , spell_care_location_site_code_of_treatment
    , spell_admission_date 
    , spell_discharge_length_of_hospital_stay 
    , SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER
    , spell_commissioning_grouping_core_hrg 
    , spell_clinical_coding_grouper_derived_primary_diagnosis  
    , spell_clinical_coding_grouper_derived_secondary_diagnosis 
    , spell_admission_admission_sub_type
    , spell_commissioning_tariff_calculation_final_price

from {{ ref('raw_sus_apc_spell') }}