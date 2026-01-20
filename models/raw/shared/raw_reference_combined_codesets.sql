{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.COMBINED_CODESETS \ndbt: source(''reference_terminology'', ''COMBINED_CODESETS'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  CODE -> code\n  CODE_DESCRIPTION -> code_description\n  SOURCE -> source"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('reference_terminology', 'COMBINED_CODESETS') }}
