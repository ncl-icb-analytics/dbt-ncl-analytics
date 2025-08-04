-- Staging model for dictionary.ServiceProviderTypes
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid,
    "ServiceProviderTypeDescription" as serviceprovidertypedescription
from {{ source('dictionary', 'ServiceProviderTypes') }}
