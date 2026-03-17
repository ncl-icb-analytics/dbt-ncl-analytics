{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'encounter']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_encounter') }}
),
cleaned as (
    select * from raw_data
    where delete_ind != 'Y' and "EncounterID" is not null
),
deduplicated as (
    select *, row_number() over (partition by "EncounterID" order by "Version" desc) as rn
    from cleaned
)
select * except rn from deduplicated where rn = 1
