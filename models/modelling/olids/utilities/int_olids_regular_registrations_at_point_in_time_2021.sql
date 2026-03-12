{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice', 'emis_comparison', 'historical']
    )
}}

/*
OLIDS Regular Registrations at Point in Time (2021)

Counts OLIDS patients with Regular registration episodes active on the
historical EMIS extract date from the 2021 seed.
*/

with emis_extract_date as (
    select extract_date as reference_date
    from {{ ref('stg_emis_list_size_2021') }}
    order by extract_date desc
    limit 1
),

patient_to_person as (
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
    select
        eoc.id,
        eoc.patient_id,
        eoc.organisation_id_publisher,
        eoc.organisation_code_publisher as practice_ods_code,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date
    from {{ ref('stg_olids_episode_of_care') }} as eoc
    cross join emis_extract_date as ed
    where eoc.episode_type_source_code = 'Regular'
        and not (eoc.episode_status_source_code = 'Left' and eoc.episode_of_care_end_date is null)
        and eoc.episode_of_care_start_date <= ed.reference_date
        and (
            eoc.episode_of_care_end_date is null
            or eoc.episode_of_care_end_date >= ed.reference_date
        )
),

active_registrations as (
    select
        r.practice_ods_code,
        ptp.person_id
    from regular_episodes as r
    cross join emis_extract_date as ed
    inner join patient_to_person as ptp
        on r.patient_id = ptp.patient_id
    left join patient_deceased_status as pds
        on r.patient_id = pds.patient_id
    where r.practice_ods_code is not null
        and (
            pds.death_date_approx is null
            or pds.death_date_approx > ed.reference_date
        )
    qualify row_number() over (
        partition by ptp.person_id, r.practice_ods_code
        order by r.id
    ) = 1
)

select
    practice_ods_code,
    count(distinct person_id) as regular_registered_patients,
    (select reference_date from emis_extract_date) as snapshot_date
from active_registrations
group by practice_ods_code
