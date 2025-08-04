-- Staging model for dictionary.PODTeams
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PODTeamID" as sk_podteamid,
    "PODTeamCode" as podteamcode,
    "PODTeamName" as podteamname,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsTestOrganisation" as istestorganisation,
    "Region" as region
from {{ source('dictionary', 'PODTeams') }}
