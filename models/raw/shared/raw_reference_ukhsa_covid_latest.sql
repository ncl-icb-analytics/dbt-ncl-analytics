{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.UKHSA_COVID_LATEST \ndbt: source(''reference_terminology'', ''UKHSA_COVID_LATEST'') \nColumns:\n  CODING_SCHEME -> coding_scheme\n  LIBRARY -> library\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_DESCRIPTION -> snomed_description\n  CODE_VALIDATED -> code_validated"
    )
}}
select
    "CODING_SCHEME" as coding_scheme,
    "LIBRARY" as library,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "CODE_VALIDATED" as code_validated
from {{ source('reference_terminology', 'UKHSA_COVID_LATEST') }}
