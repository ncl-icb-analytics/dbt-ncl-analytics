-- Raw layer model for dictionary_dbo.ConsultantProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ConsultantID" as sk_consultant_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_SpecialtyID" as sk_specialty_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ConsultantProvider') }}
