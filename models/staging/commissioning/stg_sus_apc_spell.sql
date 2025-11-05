{{
    config(materialized = 'view')
}}

with valid_orgs as(
    select organisation_code 
    from {{ ref('stg_dictionary_dbo_organisation') }} 
    where SK_ORGANISATION_TYPE_ID = 41
),
core_data as(
    select *
    from {{ ref('raw_sus_apc_spell') }} as core
    qualify row_number() over (
        partition by primarykey_id
        order by system_transaction_cds_activity_date desc
        ) = 1
)

select core.primarykey_id
    , core.spell_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , core.spell_care_location_site_code_of_treatment
    , core.spell_admission_date
    , core.spell_admission_method
    , core.spell_admission_patient_classification
    , core.spell_discharge_date
    , core.spell_discharge_length_of_hospital_stay
    , case when vo.organisation_code is not null 
        then core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER 
        else left(core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER, 3) 
        end as SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER
    , core.spell_commissioning_grouping_core_hrg
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis
    , core.spell_clinical_coding_grouper_derived_secondary_diagnosis
    , core.spell_clinical_coding_grouper_derived_dominant_procedure
    , core.spell_admission_admission_sub_type
    , core.spell_admission_admission_type

    //Added SpecComm
    , core.spell_commissioning_pss_grouping_national_programme_code as spec_comm
    , core.spell_commissioning_tariff_calculation_final_price

from core_data as core

left join valid_orgs vo 
    on core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = vo.organisation_code

