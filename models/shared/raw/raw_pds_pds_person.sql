-- Raw layer model for pds.PDS_Person
-- Source: "DATA_LAKE"."PDS"
-- Description: Personal Demographics Service data
-- This is a 1:1 passthrough from source with standardized column names
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "YearMonth_Of_Birth" as year_month_of_birth,
    "Gender" as gender,
    "Date of Death" as date_of_death,
    "Death Status" as death_status,
    "Preferred Language" as preferred_language,
    "Interpreter required" as interpreter_required,
    "Person Business Effective From Date" as person_business_effective_from_date,
    "Person Business Effective To Date" as person_business_effective_to_date
from {{ source('pds', 'PDS_Person') }}
