{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.INTERPRETER_REQUIRED \ndbt: source(''reference_lookup_ncl'', ''INTERPRETER_REQUIRED'') \nColumns:\n  ID -> id\n  INTERPRETER_REQUIRED -> interpreter_required\n  INTERPRETER_REQUIRED_FLAG -> interpreter_required_flag"
    )
}}
select
    "ID" as id,
    "INTERPRETER_REQUIRED" as interpreter_required,
    "INTERPRETER_REQUIRED_FLAG" as interpreter_required_flag
from {{ source('reference_lookup_ncl', 'INTERPRETER_REQUIRED') }}
