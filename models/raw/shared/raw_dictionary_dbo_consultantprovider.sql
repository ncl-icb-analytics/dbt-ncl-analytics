{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ConsultantProvider \ndbt: source(''dictionary_dbo'', ''ConsultantProvider'') \nColumns:\n  SK_ConsultantID -> sk_consultant_id\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_SpecialtyID -> sk_specialty_id\n  StartDate -> start_date\n  EndDate -> end_date\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_ConsultantID" as sk_consultant_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_SpecialtyID" as sk_specialty_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ConsultantProvider') }}
