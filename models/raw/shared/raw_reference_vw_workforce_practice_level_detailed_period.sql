{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.VW_WORKFORCE_PRACTICE_LEVEL_DETAILED_PERIOD \ndbt: source(''reference_analyst_managed'', ''VW_WORKFORCE_PRACTICE_LEVEL_DETAILED_PERIOD'') \nColumns:\n  SOURCE_FILE -> source_file\n  PERIOD -> period"
    )
}}
select
    "SOURCE_FILE" as source_file,
    "PERIOD" as period
from {{ source('reference_analyst_managed', 'VW_WORKFORCE_PRACTICE_LEVEL_DETAILED_PERIOD') }}
