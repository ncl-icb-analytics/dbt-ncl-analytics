{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'demographics']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_demographics') }}
),
cleaned as (
    select * from raw_data
    where delete_ind != 'Y' and "PersonID" is not null
),
deduplicated as (
    select *,
        row_number() over (partition by "PersonID" order by "Version" desc) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
