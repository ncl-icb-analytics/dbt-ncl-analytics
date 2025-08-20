-- Staging model for dictionary_dbo.PODTeams
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PODTeamID" as sk_podteamid,
    "PODTeamCode" as podteamcode,
    "PODTeamName" as podteamname,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsTestOrganisation" as istestorganisation,
    "Region" as region
from {{ source('dictionary_dbo', 'PODTeams') }}
