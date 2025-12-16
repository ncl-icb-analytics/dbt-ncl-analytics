-- Raw layer model for reference_lookup_ncl.CAREHOME_TYPES
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "CAREHOME_CODE" as carehome_code,
    "CAREHOME_NAME" as carehome_name,
    "CAREHOME_SERVICETYPE" as carehome_servicetype,
    "CAREHOME_SPECIALISM" as carehome_specialism,
    "CAREHOME_LA" as carehome_la,
    "CAREHOME_REGION" as carehome_region,
    "CQC_LOCATION_ID" as cqc_location_id,
    "CQC_PROVIDER_ID" as cqc_provider_id
from {{ source('reference_lookup_ncl', 'CAREHOME_TYPES') }}
