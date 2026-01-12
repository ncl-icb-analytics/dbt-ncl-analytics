-- Raw layer model for reference_lookup_ncl.INTERPRETER_REQUIRED
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "INTERPRETER_REQUIRED" as interpreter_required,
    "INTERPRETER_REQUIRED_FLAG" as interpreter_required_flag
from {{ source('reference_lookup_ncl', 'INTERPRETER_REQUIRED') }}
