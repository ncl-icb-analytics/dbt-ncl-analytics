-- Raw layer model for reference_lookup_ncl.CONSULTANTCONNECT_ORG_TRUSTCODE_LOOKUP
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "Trust" as trust,
    "Organisation_Name" as organisation_name,
    "Organisation_Code" as organisation_code
from {{ source('reference_lookup_ncl', 'CONSULTANTCONNECT_ORG_TRUSTCODE_LOOKUP') }}
