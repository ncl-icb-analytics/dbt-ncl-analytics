-- Staging model for dictionary.PODCommissioners
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_CommissionerID" as sk_commissionerid,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PCTID" as sk_pctid,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner
from {{ source('dictionary', 'PODCommissioners') }}
