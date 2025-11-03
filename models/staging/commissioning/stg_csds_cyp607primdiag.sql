{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp607primdiag'),
            partition_cols = ['unique_service_request_identifier',
                              'primary_diagnosis_coded_clinical_entry',
                              'diagnosis_scheme_in_use_community_care',
                              'diagnosis_date']
        )
    }}
)

SELECT
    unique_service_request_identifier,
    person_id,
    diagnosis_scheme_in_use_community_care,
    primary_diagnosis_coded_clinical_entry,
    diagnosis_date,
    record_number,
    organisation_identifier_code_of_provider,
    cyp607_unique_id

FROM deduplicated