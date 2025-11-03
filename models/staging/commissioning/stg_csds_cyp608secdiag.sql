{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp608secdiag'),
            partition_cols = ['unique_service_request_identifier',
                              'secondary_diagnosis_coded_clinical_entry',
                              'diagnosis_scheme_in_use_community_care',
                              'diagnosis_date']
        )
    }}
)

SELECT
    unique_service_request_identifier,
    person_id,
    diagnosis_scheme_in_use_community_care,
    secondary_diagnosis_coded_clinical_entry,
    diagnosis_date,
    record_number,
    organisation_identifier_code_of_provider,
    cyp608_unique_id

FROM deduplicated