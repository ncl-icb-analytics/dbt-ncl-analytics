{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.STPByCommissioner \ndbt: source(''dictionary_dbo'', ''STPByCommissioner'') \nColumns:\n  SK_CommissionerID -> sk_commissioner_id\n  SK_CommissionerGroupID -> sk_commissioner_group_id\n  SK_CommissionerOrgID -> sk_commissioner_org_id\n  CommissionerCode -> commissioner_code\n  CommissionerName -> commissioner_name\n  SK_PODTeamID -> sk_pod_team_id\n  SK_PODTeamGroupID -> sk_pod_team_group_id\n  PODTeamCode -> pod_team_code\n  PODTeamName -> pod_team_name\n  SK_STPID -> sk_stpid\n  SK_STPGroupID -> sk_stp_group_id\n  STPCode -> stp_code\n  STPName -> stp_name\n  SK_PCTID -> sk_pctid\n  NumberOfPractices -> number_of_practices\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_CommissionerGroupID" as sk_commissioner_group_id,
    "SK_CommissionerOrgID" as sk_commissioner_org_id,
    "CommissionerCode" as commissioner_code,
    "CommissionerName" as commissioner_name,
    "SK_PODTeamID" as sk_pod_team_id,
    "SK_PODTeamGroupID" as sk_pod_team_group_id,
    "PODTeamCode" as pod_team_code,
    "PODTeamName" as pod_team_name,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stp_group_id,
    "STPCode" as stp_code,
    "STPName" as stp_name,
    "SK_PCTID" as sk_pctid,
    "NumberOfPractices" as number_of_practices,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'STPByCommissioner') }}
