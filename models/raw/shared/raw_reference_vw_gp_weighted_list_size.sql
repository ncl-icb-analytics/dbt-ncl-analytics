{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.VW_GP_WEIGHTED_LIST_SIZE \ndbt: source(''reference_analyst_managed'', ''VW_GP_WEIGHTED_LIST_SIZE'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  BOROUGH -> borough\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  WEIGHTED_LIST_SIZE -> weighted_list_size"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "BOROUGH" as borough,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "WEIGHTED_LIST_SIZE" as weighted_list_size
from {{ source('reference_analyst_managed', 'VW_GP_WEIGHTED_LIST_SIZE') }}
