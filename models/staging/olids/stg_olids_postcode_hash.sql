select
    -- Primary key (compound)
    postcode_hash,

    -- Business columns
    primary_care_organisation,
    local_authority_organisation,
    yr_2011_lsoa,
    yr_2011_msoa,
    yr_2021_lsoa,
    yr_2021_msoa,
    effective_from,
    effective_to,
    is_latest,

    -- Metadata
    lds_start_date_time

from {{ ref('raw_olids_postcode_hash') }}
