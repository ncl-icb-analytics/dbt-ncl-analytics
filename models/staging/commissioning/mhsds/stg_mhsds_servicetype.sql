{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs102servicetypereferredto'),
            partition_cols = ['uniq_serv_req_id']
        )
    }}
)

select
    uniq_serv_req_id
    , person_id
    , serv_team_type_ref_to_mh
    , serv_team_int_age_group
    , service_type_name
    , org_id_prov
    , uniq_submission_id
from deduplicated
