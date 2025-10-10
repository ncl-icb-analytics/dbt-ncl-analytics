-- Raw layer model for dictionary_dbo.PODTeams
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PODTeamID" as sk_pod_team_id,
    "PODTeamCode" as pod_team_code,
    "PODTeamName" as pod_team_name,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "IsTestOrganisation" as is_test_organisation,
    "Region" as region
from {{ source('dictionary_dbo', 'PODTeams') }}
