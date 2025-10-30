-- Raw layer model for reference_lookup_ncl.POPULATION_SEGMENTATION_CODE_LOOKUP
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "POPSEG_CODE" as popseg_code,
    "POPSEG_NAME" as popseg_name
from {{ source('reference_lookup_ncl', 'POPULATION_SEGMENTATION_CODE_LOOKUP') }}
