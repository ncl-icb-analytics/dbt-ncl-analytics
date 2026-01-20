{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Consultant \ndbt: source(''dictionary_dbo'', ''Consultant'') \nColumns:\n  SK_ConsultantID -> sk_consultant_id\n  GMCCode -> gmc_code\n  Surname -> surname\n  Initials -> initials\n  GMCName -> gmc_name\n  SexCode -> sex_code\n  Specialty_Function_Code -> specialty_function_code\n  Location_Organisation_Code -> location_organisation_code\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
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
