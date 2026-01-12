{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CCGByPracticeNational \ndbt: source(''dictionary_dbo'', ''CCGByPracticeNational'') \nColumns:\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  SK_OrganisationID_ServiceProvider -> sk_organisation_id_service_provider\n  ServiceProviderCode -> service_provider_code\n  ServiceProviderName -> service_provider_name\n  ProviderGroupID -> provider_group_id\n  ProviderGroup -> provider_group\n  CommissionerID -> commissioner_id\n  CommissionerGroupID -> commissioner_group_id\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  CommissionerCode -> commissioner_code\n  Commissioner -> commissioner\n  PODID -> podid\n  PODGroupID -> pod_group_id\n  PODCode -> pod_code\n  POD -> pod\n  PODName -> pod_name\n  STPID -> stpid\n  STPGroupID -> stp_group_id\n  STPCode -> stp_code\n  STP -> stp\n  StartDate -> start_date\n  EndDate -> end_date\n  CommissioningRegionCode -> commissioning_region_code\n  CommissioningRegion -> commissioning_region\n  LocalAreaTeamCode -> local_area_team_code\n  LocalAreaTeam -> local_area_team\n  CommissioningCounty -> commissioning_county\n  CommissioningCountry -> commissioning_country\n  SK_Organisation_ID_ServiceProvider -> sk_organisation_id_service_provider_1\n  SK_Organisation_ID_Commissioner -> sk_organisation_id_commissioner_1"
    )
}}
select
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_OrganisationID_ServiceProvider" as sk_organisation_id_service_provider,
    "ServiceProviderCode" as service_provider_code,
    "ServiceProviderName" as service_provider_name,
    "ProviderGroupID" as provider_group_id,
    "ProviderGroup" as provider_group,
    "CommissionerID" as commissioner_id,
    "CommissionerGroupID" as commissioner_group_id,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "CommissionerCode" as commissioner_code,
    "Commissioner" as commissioner,
    "PODID" as podid,
    "PODGroupID" as pod_group_id,
    "PODCode" as pod_code,
    "POD" as pod,
    "PODName" as pod_name,
    "STPID" as stpid,
    "STPGroupID" as stp_group_id,
    "STPCode" as stp_code,
    "STP" as stp,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "CommissioningRegionCode" as commissioning_region_code,
    "CommissioningRegion" as commissioning_region,
    "LocalAreaTeamCode" as local_area_team_code,
    "LocalAreaTeam" as local_area_team,
    "CommissioningCounty" as commissioning_county,
    "CommissioningCountry" as commissioning_country,
    "SK_Organisation_ID_ServiceProvider" as sk_organisation_id_service_provider_1,
    "SK_Organisation_ID_Commissioner" as sk_organisation_id_commissioner_1
from {{ source('dictionary_dbo', 'CCGByPracticeNational') }}
