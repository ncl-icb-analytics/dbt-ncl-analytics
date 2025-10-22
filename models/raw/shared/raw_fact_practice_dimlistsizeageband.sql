-- Raw layer model for fact_practice.DimListSizeAgeBand
-- Source: "DATA_LAKE"."FACT_PRACTICE"
-- Description: Practice fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ListSizeAgeBandID" as sk_list_size_age_band_id,
    "ListSizeAgeBand" as list_size_age_band,
    "AgeBandStarts" as age_band_starts,
    "AgeBandEnds" as age_band_ends,
    "IsStandardAgeBand" as is_standard_age_band,
    "IsExtendedAgeBand" as is_extended_age_band
from {{ source('fact_practice', 'DimListSizeAgeBand') }}
