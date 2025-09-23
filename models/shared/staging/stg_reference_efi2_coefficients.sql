-- Staging model for reference_analyst_managed.EFI2_COEFFICIENTS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules

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
