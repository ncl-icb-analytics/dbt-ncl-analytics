{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_CODES \ndbt: source(''reference_terminology'', ''LTC_LCS_CODES'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_DESCRIPTION -> snomed_description"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description
from {{ source('reference_terminology', 'LTC_LCS_CODES') }}
