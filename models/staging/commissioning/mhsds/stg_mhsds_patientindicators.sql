{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        select_latest_mhsds_state(
            mhsds_table = ref('raw_mhsds_mhs005patind'),
            partition_cols = ['unique_local_patient_id'],
            tie_breaker_cols = ['mhs005_uniq_id']
        )
    }}
)

select
    mhs005_uniq_id
    , unique_local_patient_id
    , person_id
    , cpp
    , lac_status
    , lac_legal_status
    , org_id_prov
    , uniq_submission_id
    , reporting_period_end_date
    , effective_from
from deduplicated
