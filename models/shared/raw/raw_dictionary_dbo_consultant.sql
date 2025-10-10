-- Raw layer model for dictionary_dbo.Consultant
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ConsultantID" as sk_consultant_id,
    "GMCCode" as gmc_code,
    "Surname" as surname,
    "Initials" as initials,
    "GMCName" as gmc_name,
    "SexCode" as sex_code,
    "Specialty_Function_Code" as specialty_function_code,
    "Location_Organisation_Code" as location_organisation_code,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Consultant') }}
