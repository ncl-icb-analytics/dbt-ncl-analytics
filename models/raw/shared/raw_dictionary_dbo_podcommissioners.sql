{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PODCommissioners \ndbt: source(''dictionary_dbo'', ''PODCommissioners'') \nColumns:\n  SK_CommissionerID -> sk_commissioner_id\n  SK_PODTeamID -> sk_pod_team_id\n  SK_PCTID -> sk_pctid\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner"
    )
}}
select
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_PODTeamID" as sk_pod_team_id,
    "SK_PCTID" as sk_pctid,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner
from {{ source('dictionary_dbo', 'PODCommissioners') }}
