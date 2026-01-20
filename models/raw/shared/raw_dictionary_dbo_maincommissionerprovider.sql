{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.MainCommissionerProvider \ndbt: source(''dictionary_dbo'', ''MainCommissionerProvider'') \nColumns:\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  SK_CommissionerID -> sk_commissioner_id\n  SK_OrganisationID_Provider -> sk_organisation_id_provider\n  SK_ServiceProviderID -> sk_service_provider_id\n  StartDate -> start_date\n  EndDate -> end_date"
    )
}}
select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_OrganisationID_Provider" as sk_organisation_id_provider,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'MainCommissionerProvider') }}
