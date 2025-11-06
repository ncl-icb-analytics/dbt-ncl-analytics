-- Raw layer model for reference_lookup_ncl.CONSULTANTCONNECT_ORG_GPCODE_LOOKUP
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "CleanOrganisation" as clean_organisation,
    "OrganisationCode" as organisation_code
from {{ source('reference_lookup_ncl', 'CONSULTANTCONNECT_ORG_GPCODE_LOOKUP') }}
