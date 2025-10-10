-- Raw layer model for pds.PDS_Reason_For_Removal
-- Source: "DATA_LAKE"."PDS"
-- Description: Personal Demographics Service data
-- This is a 1:1 passthrough from source with standardized column names
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Reason for Removal" as reason_for_removal,
    "Reason for Removal Business Effective From Date" as reason_for_removal_business_effective_from_date,
    "Reason for Removal Business Effective To Date" as reason_for_removal_business_effective_to_date
from {{ source('pds', 'PDS_Reason_For_Removal') }}
