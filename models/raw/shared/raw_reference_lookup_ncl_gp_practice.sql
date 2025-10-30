-- Raw layer model for reference_lookup_ncl.GP_PRACTICE
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ORGANISATION_ID" as sk_organisation_id,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PRACTICE_NAME_SHORT" as practice_name_short,
    "BOROUGH" as borough,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name
from {{ source('reference_lookup_ncl', 'GP_PRACTICE') }}
