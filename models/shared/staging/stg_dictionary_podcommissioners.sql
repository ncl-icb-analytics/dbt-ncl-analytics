-- Staging model for dictionary.PODCommissioners
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CommissionerID" as sk_commissionerid,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PCTID" as sk_pctid,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner
from {{ source('dictionary', 'PODCommissioners') }}
