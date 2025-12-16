-- Raw layer model for reference_analyst_managed.MODEL_HOSPITAL_SUBCOMPARTMENT_MAPPING
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "SubCompartment" as sub_compartment,
    "SubCompartment_Mapping" as sub_compartment_mapping
from {{ source('reference_analyst_managed', 'MODEL_HOSPITAL_SUBCOMPARTMENT_MAPPING') }}
