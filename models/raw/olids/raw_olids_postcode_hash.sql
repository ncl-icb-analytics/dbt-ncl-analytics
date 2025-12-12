-- Raw layer model for olids.POSTCODE_HASH
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "POSTCODE_HASH" as postcode_hash,
    "PRIMARY_CARE_ORGANISATION" as primary_care_organisation,
    "LOCAL_AUTHORITY_ORGANISATION" as local_authority_organisation,
    "YR2011_LSOA" as yr2011_lsoa,
    "YR2011_MSOA" as yr2011_msoa,
    "YR2021_LSOA" as yr2021_lsoa,
    "YR2021_MSOA" as yr2021_msoa,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_LATEST" as is_latest,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LAKEHOUSE_DATE_PROCESSED" as lakehouse_date_processed,
    "HIGH_WATERMARK_DATE_TIME" as high_watermark_date_time
from {{ source('olids', 'POSTCODE_HASH') }}
