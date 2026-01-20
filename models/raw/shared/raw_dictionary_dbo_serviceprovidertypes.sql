{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ServiceProviderTypes \ndbt: source(''dictionary_dbo'', ''ServiceProviderTypes'') \nColumns:\n  SK_ServiceProviderTypeID -> sk_service_provider_type_id\n  ServiceProviderTypeDescription -> service_provider_type_description"
    )
}}
select
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id,
    "ServiceProviderTypeDescription" as service_provider_type_description
from {{ source('dictionary_dbo', 'ServiceProviderTypes') }}
