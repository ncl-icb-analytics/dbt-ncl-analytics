-- Staging model for dictionary_dbo.Ethnicity
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_EthnicityID" as sk_ethnicityid,
    "BK_EthnicityCode" as bk_ethnicitycode,
    "EthnicityHESCode" as ethnicityhescode,
    "EthnicityCodeType" as ethnicitycodetype,
    "EthnicityCombinedCode" as ethnicitycombinedcode,
    "EthnicityDesc" as ethnicitydesc,
    "EthnicityDesc2" as ethnicitydesc2,
    "EthnicityDescRead" as ethnicitydescread,
    "DateStart" as datestart,
    "DateEnd" as dateend,
    "DateLastUpdate" as datelastupdate
from {{ source('dictionary_dbo', 'Ethnicity') }}
