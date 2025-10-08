
{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp101referral')
        )
    }}
)

SELECT
    unique_service_request_identifier,
    person_id,
    referral_request_received_date
FROM deduplicated