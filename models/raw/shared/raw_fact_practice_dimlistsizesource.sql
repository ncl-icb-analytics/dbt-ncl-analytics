-- Raw layer model for fact_practice.DimListSizeSource
-- Source: "DATA_LAKE"."FACT_PRACTICE"
-- Description: Practice fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ListSizeSourceID" as sk_list_size_source_id,
    "ListSizeSource" as list_size_source,
    "Description" as description
from {{ source('fact_practice', 'DimListSizeSource') }}
