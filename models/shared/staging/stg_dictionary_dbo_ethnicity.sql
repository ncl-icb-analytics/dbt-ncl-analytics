-- Staging model for dictionary_dbo.Ethnicity
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_EthnicityID" as sk_ethnicity_id,
    "BK_EthnicityCode" as bk_ethnicity_code,
    "EthnicityHESCode" as ethnicity_hes_code,
    "EthnicityCodeType" as ethnicity_code_type,
    "EthnicityCombinedCode" as ethnicity_combined_code,
    "EthnicityDesc" as ethnicity_desc,
    "EthnicityDesc2" as ethnicity_desc2,
    "EthnicityDescRead" as ethnicity_desc_read,
    "DateStart" as date_start,
    "DateEnd" as date_end,
    "DateLastUpdate" as date_last_update
from {{ source('dictionary_dbo', 'Ethnicity') }}
