{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.rownumber_id", "f.opcs_id"]) }}
    as procedure_id,
    sa.appointment_date as date,
    f.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    null::number as episodes_id,
    f.opcs_id as problem_order,
    f.rownumber_id,
    sa.sk_patient_id,
    sa.appointment_commissioning_service_agreement_provider as organisation_id,
    dict_provider.service_provider_name as organisation_name,  
    sa.appointment_care_location_site_code_of_treatment as site_id,
    dict_org.organisation_name as site_name,  -- join to reference
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from {{ ref("stg_sus_op_appointment_clinical_coding_procedure_opcs") }} f

left join
    {{ ref('raw_aic_base_athena_concept') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'

left join {{ ref("stg_sus_op_appointment") }} sa on sa.primarykey_id = f.primarykey_id

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON sa.appointment_commissioning_service_agreement_provider = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    sa.appointment_care_location_site_code_of_treatment = dict_org.organisation_code 