-- Staging model for dictionary.CommissioningLocation
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

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
from {{ source('dictionary', 'CommissioningLocation') }}
