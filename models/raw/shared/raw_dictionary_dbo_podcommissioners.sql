-- Raw layer model for dictionary_dbo.PODCommissioners
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_PODTeamID" as sk_pod_team_id,
    "SK_PCTID" as sk_pctid,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner
from {{ source('dictionary_dbo', 'PODCommissioners') }}
