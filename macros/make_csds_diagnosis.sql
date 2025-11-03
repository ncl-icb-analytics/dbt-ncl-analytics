{% macro make_csds_diagnosis(
        diagnosis_table
    ) %}

    {% if 'prim' in diagnosis_table %}
        {% set diagnosis_col = 'primary_diagnosis_coded_clinical_entry' %} 
        {% set diagnostic_flag = 'primary' %} 
    {% elif 'sec' in diagnosis_table %}
        {% set diagnosis_col = 'secondary_diagnosis_coded_clinical_entry' %} 
        {% set diagnostic_flag = 'secondary' %} 
    {% else %}
        {{ exceptions.raise_compiler_error("diagnosis_table must contain 'prim' or 'sec'") }}
    {% endif %}

    select

        bridging.sk_patient_id,
        referral.unique_service_request_identifier as referral_id,
        diag.diagnosis_date as diagnosis_date,
        'CSDS' as diagnosis_source,
        '{{ diagnostic_flag }}' as diagnostic_hierarchy,
        diag.organisation_identifier_code_of_provider as organisation_id, 
        dict_provider.service_provider_name as organisation_name,
        diag.{{ diagnosis_col }} as source_concept_code,
        c.concept_code,
        c.concept_name,
        case
            when diag.diagnosis_scheme_in_use_community_care = '02' then 'ICD-10' 
            when diag.diagnosis_scheme_in_use_community_care = '04' then 'Read Code Version 2' 
            when diag.diagnosis_scheme_in_use_community_care = '05' then 'Read Code Clinical Terms Version 3 (CTV3)' 
            when diag.diagnosis_scheme_in_use_community_care = '06' then 'SNOMED CT' 
        end as concept_vocabulary


    from {{ ref(diagnosis_table) }}  as diag

    left join {{ ref('stg_csds_cyp101referral')}} as referral
        on diag.unique_service_request_identifier = referral.unique_service_request_identifier

    left join {{ source('aic', 'BASE_ATHENA__CONCEPT')}} as c
        on  diag.{{ diagnosis_col }} = c.concept_code

    left join {{ ref('stg_csds_bridging') }} AS bridging 
        on diag.person_id = bridging.person_id

    left join {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
        on diag.organisation_identifier_code_of_provider = dict_provider.service_provider_full_code

{% endmacro %}