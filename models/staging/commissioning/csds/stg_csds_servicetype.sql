{{
    config(materialized = 'table')
}}

with deduplicated as (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp102servicetypereferredto'),
            partition_cols = ['unique_service_request_identifier']
        )
    }}
)

select
    unique_service_request_identifier
    , person_id
    , service_or_team_type_referred_to_community_care as team_type_code
from deduplicated
