{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.EFI2_COEFFICIENTS \ndbt: source(''reference_analyst_managed'', ''EFI2_COEFFICIENTS'') \nColumns:\n  MODEL_NAME -> model_name\n  VARIABLE_NAME -> variable_name\n  VARIABLE_CATEGORY -> variable_category\n  COEFFICIENT -> coefficient\n  TRANSFORMED_COEFFICIENT -> transformed_coefficient\n  DESCRIPTION -> description\n  VARIABLE_TYPE -> variable_type\n  MODEL_DESCRIPTION -> model_description"
    )
}}
select
    "MODEL_NAME" as model_name,
    "VARIABLE_NAME" as variable_name,
    "VARIABLE_CATEGORY" as variable_category,
    "COEFFICIENT" as coefficient,
    "TRANSFORMED_COEFFICIENT" as transformed_coefficient,
    "DESCRIPTION" as description,
    "VARIABLE_TYPE" as variable_type,
    "MODEL_DESCRIPTION" as model_description
from {{ source('reference_analyst_managed', 'EFI2_COEFFICIENTS') }}
