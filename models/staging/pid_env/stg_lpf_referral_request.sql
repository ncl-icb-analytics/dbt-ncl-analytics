{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'referral']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_referral_request') }}
),
cleaned as (
    select * from raw_data
    where delete_ind != 'Y' and "ReferralRequestID" is not null
),
deduplicated as (
    select *,
        row_number() over (partition by "ReferralRequestID" order by "Version" desc) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
