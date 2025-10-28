-- Raw layer model for aic.BASE_OLIDS__POSTCODE_HASH
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "POSTCODE_HASH" as postcode_hash,
    "PRIMARY_CARE_ORGANISATION" as primary_care_organisation,
    "LOCAL_AUTHORITY_ORGANISATION" as local_authority_organisation,
    "YR_2011_LSOA" as yr_2011_lsoa,
    "YR_2011_MSOA" as yr_2011_msoa,
    "YR_2021_LSOA" as yr_2021_lsoa,
    "YR_2021_MSOA" as yr_2021_msoa,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_LATEST" as is_latest,
    "LDS_START_DATE_TIME" as lds_start_date_time
from {{ source('aic', 'BASE_OLIDS__POSTCODE_HASH') }}
