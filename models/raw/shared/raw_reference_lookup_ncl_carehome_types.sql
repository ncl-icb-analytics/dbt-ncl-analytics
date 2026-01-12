{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.CAREHOME_TYPES \ndbt: source(''reference_lookup_ncl'', ''CAREHOME_TYPES'') \nColumns:\n  CAREHOME_CODE -> carehome_code\n  CAREHOME_NAME -> carehome_name\n  CAREHOME_SERVICETYPE -> carehome_servicetype\n  CAREHOME_SPECIALISM -> carehome_specialism\n  CAREHOME_LA -> carehome_la\n  CAREHOME_REGION -> carehome_region\n  CQC_LOCATION_ID -> cqc_location_id\n  CQC_PROVIDER_ID -> cqc_provider_id"
    )
}}
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
