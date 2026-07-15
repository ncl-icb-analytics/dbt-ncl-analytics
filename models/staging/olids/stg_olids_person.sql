{{ config(materialized='table') }}

-- Native OLIDS PERSON can carry multiple rows per id when a person has multiple
-- practice registrations recorded. Dedup to one row per person using the most
-- recent gp_registration_date (with deterministic tie-breakers). Materialised
-- as a table so the window-function dedup runs once at build time, not on
-- every downstream query.
select
    -- Primary key
    id,
    person_uuid,

    -- Demographics
    date_of_birth_year as birth_year,
    date_of_birth_month as birth_month,
    date_of_death_year as death_year,
    date_of_death_month as death_month,
    death_notification_status,
    postcode, -- REVIEW: plain postcode replaces the removed postcode_hash
    patient_flagged_sensitive as sensitivity_flag,

    -- Registration
    gp_practice_code,
    CAST(gp_registration_date AS DATE) AS gp_registration_date,
    CAST(as_at_date AS DATE) AS as_at_date,

    -- Contact / nominated services
    preferred_contact_method,
    nominated_pharmacy,
    dispensing_doctor,
    medical_appliance_supplier,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,
    lds_transform_datetime
from {{ ref('raw_olids_person') }}
qualify row_number() over (
    partition by id
    order by gp_registration_date desc nulls last,
             lds_transform_datetime desc nulls last,
             gp_practice_code desc nulls last
) = 1
