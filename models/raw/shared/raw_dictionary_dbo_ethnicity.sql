{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Ethnicity \ndbt: source(''dictionary_dbo'', ''Ethnicity'') \nColumns:\n  SK_EthnicityID -> sk_ethnicity_id\n  BK_EthnicityCode -> bk_ethnicity_code\n  EthnicityHESCode -> ethnicity_hes_code\n  EthnicityCodeType -> ethnicity_code_type\n  EthnicityCombinedCode -> ethnicity_combined_code\n  EthnicityDesc -> ethnicity_desc\n  EthnicityDesc2 -> ethnicity_desc2\n  EthnicityDescRead -> ethnicity_desc_read\n  DateStart -> date_start\n  DateEnd -> date_end\n  DateLastUpdate -> date_last_update"
    )
}}
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
