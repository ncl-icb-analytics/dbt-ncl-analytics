-- Raw layer model for fact_practice.FactListSize
-- Source: "DATA_LAKE"."FACT_PRACTICE"
-- Description: Practice fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ListSizeSourceID" as sk_list_size_source_id,
    "SK_OrganisationID" as sk_organisation_id,
    "Period" as period,
    "Value" as value
from {{ source('fact_practice', 'FactListSize') }}
