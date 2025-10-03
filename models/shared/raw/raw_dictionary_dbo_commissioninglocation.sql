-- Raw layer model for dictionary_dbo.CommissioningLocation
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
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
