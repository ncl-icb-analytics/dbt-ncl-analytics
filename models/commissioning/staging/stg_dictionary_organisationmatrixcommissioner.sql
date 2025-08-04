-- Staging model for dictionary.OrganisationMatrixCommissioner
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "SK_OrganisationID_STP" as sk_organisationid_stp
from {{ source('dictionary', 'OrganisationMatrixCommissioner') }}
