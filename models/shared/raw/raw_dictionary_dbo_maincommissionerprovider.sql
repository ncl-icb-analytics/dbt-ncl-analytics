-- Raw layer model for dictionary_dbo.MainCommissionerProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_OrganisationID_Provider" as sk_organisation_id_provider,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'MainCommissionerProvider') }}
