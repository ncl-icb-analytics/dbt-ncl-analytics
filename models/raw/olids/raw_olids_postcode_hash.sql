{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.POSTCODE_HASH \ndbt: source(''olids'', ''POSTCODE_HASH'') \nColumns:\n  ID -> id\n  POSTCODE_HASH -> postcode_hash\n  PRIMARY_CARE_ORGANISATION -> primary_care_organisation\n  LOCAL_AUTHORITY_ORGANISATION -> local_authority_organisation\n  YR2011_LSOA -> yr2011_lsoa\n  YR2011_MSOA -> yr2011_msoa\n  YR2021_LSOA -> yr2021_lsoa\n  YR2021_MSOA -> yr2021_msoa\n  EFFECTIVE_FROM -> effective_from\n  EFFECTIVE_TO -> effective_to\n  IS_LATEST -> is_latest\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LAKEHOUSE_DATE_PROCESSED -> lakehouse_date_processed\n  HIGH_WATERMARK_DATE_TIME -> high_watermark_date_time"
    )
}}
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
