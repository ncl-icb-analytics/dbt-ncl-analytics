-- Raw layer model for dictionary_dbo.STPByCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
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
