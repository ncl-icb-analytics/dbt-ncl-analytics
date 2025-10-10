-- Raw layer model for pds.PDS_Person_Merger
-- Source: "DATA_LAKE"."PDS"
-- Description: Personal Demographics Service data
-- This is a 1:1 passthrough from source with standardized column names
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Pseudo Superseded NHS Number" as pseudo_superseded_nhs_number,
    "Person Merger Business Effective From Date" as person_merger_business_effective_from_date,
    "Person Merger Business Effective To Date" as person_merger_business_effective_to_date
from {{ source('pds', 'PDS_Person_Merger') }}
