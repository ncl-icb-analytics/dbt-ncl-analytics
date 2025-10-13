select
    -- Primary key
    id,

    -- Business columns
    organisation_id,
    patient_id,
    person_id,
    practitioner_in_role_id,
    schedule_id,
    start_date,
    planned_duration,
    actual_duration,
    appointment_status_concept_id,
    patient_wait,
    patient_delay,
    date_time_booked,
    date_time_sent_in,
    date_time_left,
    cancelled_date,
    type,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    booking_method_concept_id,
    contact_mode_concept_id,
    is_blocked,
    national_slot_category_name,
    context_type,
    service_setting,
    national_slot_category_description,
    csds_care_contact_identifier,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_appointment') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
