{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CommissioningLocation \ndbt: source(''dictionary_dbo'', ''CommissioningLocation'') \nColumns:\n  SK_CCGLocationId -> sk_ccg_location_id\n  CommissionerCode -> commissioner_code\n  CommissioningRegionCode -> commissioning_region_code\n  CommissioningRegion -> commissioning_region\n  LocalAreaTeamCode -> local_area_team_code\n  LocalAreaTeam -> local_area_team\n  CommissioningCounty -> commissioning_county\n  CommissioningCountry -> commissioning_country\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  StartDate -> start_date\n  EndDate -> end_date"
    )
}}
select
    "SK_CCGLocationId" as sk_ccg_location_id,
    "CommissionerCode" as commissioner_code,
    "CommissioningRegionCode" as commissioning_region_code,
    "CommissioningRegion" as commissioning_region,
    "LocalAreaTeamCode" as local_area_team_code,
    "LocalAreaTeam" as local_area_team,
    "CommissioningCounty" as commissioning_county,
    "CommissioningCountry" as commissioning_country,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'CommissioningLocation') }}
