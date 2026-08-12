{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs005patind'),
            partition_cols = ['mhs005_uniq_id']
        )
    }}
)

-- REVIEW: MHS005UniqID is the source natural key and is used instead of a
-- person and reporting-period composite.
select
    mhs005_uniq_id
    , person_id
    , cpp
    , lac_status
    , lac_legal_status
    , org_id_prov
    , reporting_period_end_date
from deduplicated
