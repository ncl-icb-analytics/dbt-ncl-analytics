-- Staging model for dictionary_dbo.Ethnicity2
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_EthnicityID" as sk_ethnicityid,
    "EthnicityCategory" as ethnicitycategory,
    "EthnicityDesc" as ethnicitydesc
from {{ source('dictionary_dbo', 'Ethnicity2') }}
