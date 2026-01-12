{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.EthnicityCode \ndbt: source(''dictionary_dbo'', ''EthnicityCode'') \nColumns:\n  SK_EthnicityID -> sk_ethnicity_id\n  EthnicityCodeType -> ethnicity_code_type\n  EthnicCategoryCode -> ethnic_category_code\n  EthnicGroupCode -> ethnic_group_code\n  ICCode -> ic_code\n  PDSEthnicCategoryCode -> pds_ethnic_category_code\n  ReadCode -> read_code\n  SDECode -> sde_code\n  Description -> description\n  Priority -> priority\n  Snomed -> snomed"
    )
}}
select
    "SK_EthnicityID" as sk_ethnicity_id,
    "EthnicityCodeType" as ethnicity_code_type,
    "EthnicCategoryCode" as ethnic_category_code,
    "EthnicGroupCode" as ethnic_group_code,
    "ICCode" as ic_code,
    "PDSEthnicCategoryCode" as pds_ethnic_category_code,
    "ReadCode" as read_code,
    "SDECode" as sde_code,
    "Description" as description,
    "Priority" as priority,
    "Snomed" as snomed
from {{ source('dictionary_dbo', 'EthnicityCode') }}
