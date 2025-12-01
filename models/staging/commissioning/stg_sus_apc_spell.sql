{{
    config(materialized = 'view')
}}

with core_data as(
    select *
    from {{ ref('raw_sus_apc_spell') }} as core
    qualify row_number() over (
        partition by primarykey_id
        order by system_transaction_cds_activity_date desc
        ) = 1
)

select core.primarykey_id
    -- patient details at time of event
    , core.spell_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , core.spell_patient_residence_derived_postcode_district
    , core.spell_patient_residence_derived_lsoa_11
    , core.spell_patient_residence_derived_local_authority_district
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile_description
    , core.spell_patient_identity_age_on_admission
    , core.spell_patient_identity_gender
    , core.spell_patient_identity_ethnic_category
    , core.spell_patient_registration_general_practice
    , core.spell_patient_identity_spell_age
    , core.spell_patient_identity_age_on_admission
    , core.spell_patient_identity_birth_year
    , core.spell_patient_identity_birth_month
    -- spell details
    , core.spell_care_location_site_code_of_treatment
    , core.spell_admission_date
    , core.spell_admission_method
    , core.spell_admission_admission_sub_type
    , core.spell_admission_admission_type
    , core.spell_admission_patient_classification
    , core.spell_discharge_date
    , core.spell_discharge_length_of_hospital_stay

    -- diagnosis
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis
    , core.spell_clinical_coding_grouper_derived_secondary_diagnosis
    , core.spell_clinical_coding_grouper_derived_dominant_procedure
    , core.spell_admission_admission_sub_type
    , core.spell_admission_admission_type

    -- Added SpecComm
    , core.spell_commissioning_pss_grouping_national_programme_code as spec_comm

    -- commissioning and costing details
    , {{ clean_organisation_id('SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER') }} as SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER
    , core.spell_commissioning_grouping_core_hrg
    , core.spell_commissioning_tariff_calculation_final_price

from core_data as core


