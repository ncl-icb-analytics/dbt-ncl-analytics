select
    -- Primary key
    id,

    -- Identifiers
    sk_patient_id,
    nhs_number_hash,

    -- Preferences (normalised UPPER_SNAKE_CASE: spaces and hyphens -> underscore)
    -- "Opt-In" -> "OPT_IN", "National Data Opt-out" -> "NATIONAL_DATA_OPT_OUT"
    upper(replace(replace(preference_type, ' ', '_'), '-', '_')) as preference_type,
    upper(replace(replace(preference_status, ' ', '_'), '-', '_')) as preference_status,

    -- Validity
    effective_from,
    effective_to,
    is_latest,

    -- Metadata
    lds_is_deleted,
    lds_start_date_time,
    lds_datetime_data_acquired,
    lds_batch_id,
    lds_file_id,
    lds_dataset_id,
    lds_record_id,
    lakehouse_date_processed

from {{ ref('raw_olids_ndoo_hashed') }}
