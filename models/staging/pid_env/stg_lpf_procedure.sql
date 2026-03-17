{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'procedure']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_procedure') }}
),
cleaned as (
    select * from raw_data
    where delete_ind != 'Y' and "ProcedureID" is not null
),
deduplicated as (
    select *, row_number() over (partition by "ProcedureID" order by "Version" desc) as rn
    from cleaned
)
select * except rn from deduplicated where rn = 1
