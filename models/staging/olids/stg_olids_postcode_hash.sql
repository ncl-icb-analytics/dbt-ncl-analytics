select
    -- Primary key (compound)
    postcode_hash,

    -- Business columns
    primary_care_organisation,
    local_authority_organisation,
    yr2011_lsoa as yr_2011_lsoa,
    yr2011_msoa as yr_2011_msoa,
    yr2021_lsoa as yr_2021_lsoa,
    yr2021_msoa as yr_2021_msoa,
    effective_from,
    effective_to,
    is_latest,

    -- Metadata
    lds_start_date_time,
    lakehouse_date_processed

from {{ ref('raw_olids_postcode_hash') }}
