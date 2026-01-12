{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PracticeMatrix \ndbt: source(''dictionary_dbo'', ''PracticeMatrix'') \nColumns:\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  SK_ServiceProviderOrgID -> sk_service_provider_org_id\n  SK_NetworkID -> sk_network_id\n  SK_CommissionerID -> sk_commissioner_id\n  SK_CommissionerGroupID -> sk_commissioner_group_id\n  SK_CommissionerOrgID -> sk_commissioner_org_id\n  SK_PODTeamID -> sk_pod_team_id\n  SK_PODTeamGroupID -> sk_pod_team_group_id\n  SK_STPID -> sk_stpid\n  SK_STPGroupID -> sk_stp_group_id\n  SK_PCTID -> sk_pctid\n  SK_CCGLocationID -> sk_ccg_location_id\n  IsPracticeActive -> is_practice_active\n  IsPracticeDormant -> is_practice_dormant\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  NetworkCount -> network_count\n  HasGPExtract -> has_gp_extract"
    )
}}
select
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_ServiceProviderOrgID" as sk_service_provider_org_id,
    "SK_NetworkID" as sk_network_id,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_CommissionerGroupID" as sk_commissioner_group_id,
    "SK_CommissionerOrgID" as sk_commissioner_org_id,
    "SK_PODTeamID" as sk_pod_team_id,
    "SK_PODTeamGroupID" as sk_pod_team_group_id,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stp_group_id,
    "SK_PCTID" as sk_pctid,
    "SK_CCGLocationID" as sk_ccg_location_id,
    "IsPracticeActive" as is_practice_active,
    "IsPracticeDormant" as is_practice_dormant,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "NetworkCount" as network_count,
    "HasGPExtract" as has_gp_extract
from {{ source('dictionary_dbo', 'PracticeMatrix') }}
