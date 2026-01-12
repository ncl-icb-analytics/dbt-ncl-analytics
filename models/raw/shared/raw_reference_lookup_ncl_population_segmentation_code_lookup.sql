{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.POPULATION_SEGMENTATION_CODE_LOOKUP \ndbt: source(''reference_lookup_ncl'', ''POPULATION_SEGMENTATION_CODE_LOOKUP'') \nColumns:\n  POPSEG_CODE -> popseg_code\n  POPSEG_NAME -> popseg_name"
    )
}}
select
    "POPSEG_CODE" as popseg_code,
    "POPSEG_NAME" as popseg_name
from {{ source('reference_lookup_ncl', 'POPULATION_SEGMENTATION_CODE_LOOKUP') }}
