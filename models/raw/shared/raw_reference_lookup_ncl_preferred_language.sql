{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.PREFERRED_LANGUAGE \ndbt: source(''reference_lookup_ncl'', ''PREFERRED_LANGUAGE'') \nColumns:\n  CODE -> code\n  PREFERRED_LANGUAGE -> preferred_language\n  ISO_ORIGIN -> iso_origin"
    )
}}
select
    "CODE" as code,
    "PREFERRED_LANGUAGE" as preferred_language,
    "ISO_ORIGIN" as iso_origin
from {{ source('reference_lookup_ncl', 'PREFERRED_LANGUAGE') }}
