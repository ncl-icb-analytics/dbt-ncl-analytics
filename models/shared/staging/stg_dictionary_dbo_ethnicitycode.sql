-- Staging model for dictionary_dbo.EthnicityCode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
