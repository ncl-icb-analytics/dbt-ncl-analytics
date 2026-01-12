{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.NCL_NEIGHBOURHOOD \ndbt: source(''reference_lookup_ncl'', ''NCL_NEIGHBOURHOOD'') \nColumns:\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  BOROUGH -> borough"
    )
}}
select
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "BOROUGH" as borough
from {{ source('reference_lookup_ncl', 'NCL_NEIGHBOURHOOD') }}
