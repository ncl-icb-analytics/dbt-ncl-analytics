{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'diagnosis']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_diagnosis') }}
),
cleaned as (
    select * from raw_data
    where delete_ind != 'Y' and "DiagnosisID" is not null
),
deduplicated as (
    select *, row_number() over (partition by "DiagnosisID" order by "Version" desc) as rn
    from cleaned
)
select * except rn from deduplicated where rn = 1
