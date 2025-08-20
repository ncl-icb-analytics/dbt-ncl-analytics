-- Staging model for dictionary_dbo.CommissioningLocation
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CCGLocationId" as sk_ccglocationid,
    "CommissionerCode" as commissionercode,
    "CommissioningRegionCode" as commissioningregioncode,
    "CommissioningRegion" as commissioningregion,
    "LocalAreaTeamCode" as localareateamcode,
    "LocalAreaTeam" as localareateam,
    "CommissioningCounty" as commissioningcounty,
    "CommissioningCountry" as commissioningcountry,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary_dbo', 'CommissioningLocation') }}
