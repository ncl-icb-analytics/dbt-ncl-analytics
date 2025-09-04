-- Staging model for dictionary_dbo.PracticeMatrix
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
