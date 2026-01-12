{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CommissionerMatrix \ndbt: source(''dictionary_dbo'', ''CommissionerMatrix'') \nColumns:\n  SK_CommissionerID -> sk_commissioner_id\n  SK_CommissionerGroupID -> sk_commissioner_group_id\n  SK_CommissionerOrgID -> sk_commissioner_org_id\n  SK_PODTeamID -> sk_pod_team_id\n  SK_PODTeamGroupID -> sk_pod_team_group_id\n  SK_STPID -> sk_stpid\n  SK_STPGroupID -> sk_stp_group_id\n  SK_PCTID -> sk_pctid\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  NumberOfPractices -> number_of_practices"
    )
}}
select
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_CommissionerGroupID" as sk_commissioner_group_id,
    "SK_CommissionerOrgID" as sk_commissioner_org_id,
    "SK_PODTeamID" as sk_pod_team_id,
    "SK_PODTeamGroupID" as sk_pod_team_group_id,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stp_group_id,
    "SK_PCTID" as sk_pctid,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "NumberOfPractices" as number_of_practices
from {{ source('dictionary_dbo', 'CommissionerMatrix') }}
