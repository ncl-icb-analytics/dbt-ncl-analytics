-- Staging model for dictionary.Ethnicity
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

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
from {{ source('dictionary', 'Ethnicity') }}
