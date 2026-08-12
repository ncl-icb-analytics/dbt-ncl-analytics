{{
     config(
        materialized = 'table',
        tags=['mhsds']
        )
}}

with deduplicated as (
{{
    deduplicate_mhsds(
        mhsds_table = ref('raw_mhsds_mhs204indirectactivity'),
        partition_cols = ['mhs204_uniq_id']
    )
}} )

select
    mhs204_uniq_id
    , uniq_serv_req_id
    , person_id
    , indirect_act_date
    , duration_indirect_act
    , care_prof_team_local_id
    , other_care_prof_team_local_id
from deduplicated
