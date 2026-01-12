{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Ethnicity2 \ndbt: source(''dictionary_dbo'', ''Ethnicity2'') \nColumns:\n  SK_EthnicityID -> sk_ethnicity_id\n  EthnicityCategory -> ethnicity_category\n  EthnicityDesc -> ethnicity_desc"
    )
}}
select
    "SK_EthnicityID" as sk_ethnicity_id,
    "EthnicityCategory" as ethnicity_category,
    "EthnicityDesc" as ethnicity_desc
from {{ source('dictionary_dbo', 'Ethnicity2') }}
