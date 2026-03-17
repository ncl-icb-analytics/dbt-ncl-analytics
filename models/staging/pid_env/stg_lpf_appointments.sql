{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'appointments']
    )
}}

/*
Staging: LPF Appointments

Cleans and standardizes raw appointments data from local provider flows.
Removes deleted records and null appointment IDs.
Deduplicates by taking the latest version of each appointment record.

Source: raw_lpf_appointments
*/

with raw_data as (
    select * from {{ ref('raw_lpf_appointments') }}
),

cleaned as (
    select * from raw_data
    where delete_ind != 'Y'
        and "AppointmentID" is not null
),

deduplicated as (
    select
        *,
        row_number() over (partition by "AppointmentID" order by "Version" desc) as rn
    from cleaned
)

select * exclude (rn) from deduplicated where rn = 1
