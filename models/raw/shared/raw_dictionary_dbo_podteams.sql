{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PODTeams \ndbt: source(''dictionary_dbo'', ''PODTeams'') \nColumns:\n  SK_PODTeamID -> sk_pod_team_id\n  PODTeamCode -> pod_team_code\n  PODTeamName -> pod_team_name\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  IsTestOrganisation -> is_test_organisation\n  Region -> region"
    )
}}
select
    "SK_PODTeamID" as sk_pod_team_id,
    "PODTeamCode" as pod_team_code,
    "PODTeamName" as pod_team_name,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "IsTestOrganisation" as is_test_organisation,
    "Region" as region
from {{ source('dictionary_dbo', 'PODTeams') }}
