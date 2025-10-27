
{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp101referral'),
            partition_cols = ['unique_service_request_identifier']
        )
    }}
)

SELECT
    unique_service_request_identifier,
    person_id,
    referral_request_received_date,
    primary_reason_for_referral_community_care,
    service_discharge_date,
    priority_type_code

FROM deduplicated