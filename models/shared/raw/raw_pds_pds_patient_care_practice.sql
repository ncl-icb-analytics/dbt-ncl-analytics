-- Raw layer model for pds.PDS_Patient_Care_Practice
-- Source: "DATA_LAKE"."PDS"
-- Description: Personal Demographics Service data
-- This is a 1:1 passthrough from source with standardized column names
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Primary Care Provider" as primary_care_provider,
    "Primary Care Provider Business Effective From Date" as primary_care_provider_business_effective_from_date,
    "Primary Care Provider Business Effective To Date" as primary_care_provider_business_effective_to_date,
    "Reason for Removal" as reason_for_removal,
    "derCcgOfRegistration" as der_ccg_of_registration,
    "derCurrentCcgOfRegistration" as der_current_ccg_of_registration,
    "derIcbOfRegistration" as der_icb_of_registration,
    "derCurrentIcbOfRegistration" as der_current_icb_of_registration
from {{ source('pds', 'PDS_Patient_Care_Practice') }}
