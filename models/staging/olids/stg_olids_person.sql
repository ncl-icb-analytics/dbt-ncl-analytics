-- Native OLIDS PERSON can carry multiple rows per id when a person has multiple
-- practice registrations recorded. Dedup to one row per person using the most
-- recent gp_registration_date (with deterministic tie-breakers).
select
    -- Primary key
    id,
    person_uuid,

    -- Identifiers
    matched_nhs_no_hash,
    composite_id,

    -- Demographics
    birth_year,
    birth_month,
    death_year,
    death_month,
    death_notification_status,
    postcode_hash,
    sensitivity_flag,

    -- Registration
    gp_practice_code,
    gp_registration_date,
    as_at_date,

    -- Contact / nominated services
    preferred_contact_method,
    nominated_pharmacy,
    dispensing_doctor,
    medical_appliance_supplier,

    -- Metadata
    lds_is_deleted,
    lds_start_date_time,
    lds_datetime_data_acquired

from {{ ref('raw_olids_person') }}
qualify row_number() over (
    partition by id
    order by gp_registration_date desc nulls last,
             lds_start_date_time desc nulls last,
             gp_practice_code desc nulls last
) = 1
