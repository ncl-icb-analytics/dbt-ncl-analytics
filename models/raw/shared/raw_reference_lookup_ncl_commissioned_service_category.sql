{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.COMMISSIONED_SERVICE_CATEGORY \ndbt: source(''reference_lookup_ncl'', ''COMMISSIONED_SERVICE_CATEGORY'') \nColumns:\n  COMMISSIONED_SERVICE_CATEGORY_CODE -> commissioned_service_category_code\n  FULL_DESCRIPTION -> full_description\n  SHORTER_DESCRIPTION -> shorter_description"
    )
}}
select
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "FULL_DESCRIPTION" as full_description,
    "SHORTER_DESCRIPTION" as shorter_description
from {{ source('reference_lookup_ncl', 'COMMISSIONED_SERVICE_CATEGORY') }}
