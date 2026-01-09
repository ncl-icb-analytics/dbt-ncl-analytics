-- Raw layer model for reference_lookup_ncl.PREFERRED_LANGUAGE
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "CODE" as code,
    "PREFERRED_LANGUAGE" as preferred_language,
    "ISO_ORIGIN" as iso_origin
from {{ source('reference_lookup_ncl', 'PREFERRED_LANGUAGE') }}
