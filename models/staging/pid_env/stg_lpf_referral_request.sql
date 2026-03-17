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
    where delete_ind != 'Y' and referral_request_id is not null
),
deduplicated as (
    select *,
        row_number() over (partition by referral_request_id order by version desc) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
