-- Staging model for dictionary_dbo.CCGByPracticeNational
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
