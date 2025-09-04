-- Staging model for dictionary_dbo.Ethnicity2
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_EthnicityID" as sk_ethnicity_id,
    "EthnicityCategory" as ethnicity_category,
    "EthnicityDesc" as ethnicity_desc
from {{ source('dictionary_dbo', 'Ethnicity2') }}
