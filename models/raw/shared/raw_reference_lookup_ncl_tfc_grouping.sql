-- Raw layer model for reference_lookup_ncl.TFC_GROUPING
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "CODE" as code,
    "DESCRIPTION" as description,
    "GROUPING" as grouping
from {{ source('reference_lookup_ncl', 'TFC_GROUPING') }}
