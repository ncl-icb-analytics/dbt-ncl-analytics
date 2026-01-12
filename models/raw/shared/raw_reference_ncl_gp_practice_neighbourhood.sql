{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.NCL_GP_PRACTICE_NEIGHBOURHOOD \ndbt: source(''reference_analyst_managed'', ''NCL_GP_PRACTICE_NEIGHBOURHOOD'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code
from {{ source('reference_analyst_managed', 'NCL_GP_PRACTICE_NEIGHBOURHOOD') }}
