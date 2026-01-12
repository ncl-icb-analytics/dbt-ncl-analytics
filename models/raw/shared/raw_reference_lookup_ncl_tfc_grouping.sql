{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.TFC_GROUPING \ndbt: source(''reference_lookup_ncl'', ''TFC_GROUPING'') \nColumns:\n  CODE -> code\n  DESCRIPTION -> description\n  GROUPING -> grouping"
    )
}}
select
    "CODE" as code,
    "DESCRIPTION" as description,
    "GROUPING" as grouping
from {{ source('reference_lookup_ncl', 'TFC_GROUPING') }}
