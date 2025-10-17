-- Raw layer model for fact_practice.DimListSizeAge
-- Source: "DATA_LAKE"."FACT_PRACTICE"
-- Description: Practice fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ListSizeAgeID" as sk_list_size_age_id,
    "ListSizeAge" as list_size_age,
    "AgeStarts" as age_starts,
    "AgeEnds" as age_ends
from {{ source('fact_practice', 'DimListSizeAge') }}
