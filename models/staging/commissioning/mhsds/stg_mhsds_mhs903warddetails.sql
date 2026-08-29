{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}
-- Ward details join to MHS502 by provider, submission and ward code.
with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs903warddetails')) }}
)

select *
from accepted_records
qualify row_number() over (
    partition by org_id_prov, uniq_submission_id, ward_code
    order by row_number desc, mhs903_uniq_id desc
) = 1
