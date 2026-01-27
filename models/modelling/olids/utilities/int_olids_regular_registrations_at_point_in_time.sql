{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice', 'emis_comparison']
    )
}}

/*
OLIDS Regular Registrations at Point in Time

Counts OLIDS patients with Regular registration episodes active at a fixed point in time.
Filters to Regular episode type only (excludes Temporary, Emergency, Private, etc.)
Used for EMIS list size comparison validation.

Snapshot Date: 04/11/2025 (from EMIS extract)
Episode Type: Regular only
Patient Counting: Deduplicated by person_id (handles patient ID mergers)
Deceased Handling: Excludes patients deceased on or before snapshot date
*/

with emis_extract_date as (
    -- Get reference date from EMIS staging model
    select extract_date as reference_date
    from {{ ref('stg_emis_list_size') }}
    limit 1
),

patient_to_person as (
    -- Map patient_id to canonical person_id
    select
        pp.patient_id,
        pp.person_id
    from {{ ref('stg_olids_patient_person') }} as pp
    where pp.patient_id is not null
        and pp.person_id is not null
),

patient_deceased_status as (
    select
        patient_id,
        is_deceased,
        death_date_approx
    from {{ ref('int_patient_deceased_status') }}
),

regular_episodes as (
    -- Get Regular registration episodes
    select
        eoc.id,
        eoc.patient_id,
        eoc.organisation_id,
        eoc.record_owner_organisation_code as practice_ods_code,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date
    from {{ ref('stg_olids_episode_of_care') }} as eoc
    cross join emis_extract_date as ed
    where eoc.episode_type_source_code = 'Regular'
        -- Exclude Left episodes with no end date (DQ issue: marked Left but never closed)
        and not (eoc.episode_status_source_code = 'Left' and eoc.episode_of_care_end_date is null)
        -- Episode active on reference date (inclusive end date boundary)
        and eoc.episode_of_care_start_date <= ed.reference_date
        and (
            eoc.episode_of_care_end_date is null
            or eoc.episode_of_care_end_date >= ed.reference_date
        )
),

active_registrations as (
    -- Join with person mapping and filter out deceased patients
    select
        r.practice_ods_code,
        ptp.person_id,
        p.sk_patient_id
    from regular_episodes as r
    cross join emis_extract_date as ed
    inner join patient_to_person as ptp
        on r.patient_id = ptp.patient_id
    left join {{ ref('stg_olids_patient') }} as p
        on r.patient_id = p.id
    left join patient_deceased_status as pds
        on r.patient_id = pds.patient_id
    where r.practice_ods_code is not null
        -- Exclude deceased patients as of reference date
        and (
            pds.death_date_approx is null
            or pds.death_date_approx > ed.reference_date
        )
    qualify row_number() over (
        partition by ptp.person_id, r.practice_ods_code
        order by r.id
    ) = 1
)

-- Count distinct persons per practice
select
    practice_ods_code,
    count(distinct person_id) as regular_registered_patients,
    (select reference_date from emis_extract_date) as snapshot_date
from active_registrations
group by practice_ods_code
