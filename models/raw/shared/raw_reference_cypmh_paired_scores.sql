{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CYPMH_PAIRED_SCORES \ndbt: source(''reference_analyst_managed'', ''CYPMH_PAIRED_SCORES'') \nColumns:\n  ORGANISATION_CODE -> organisation_code\n  ORGANISATION___NAME -> organisation_name\n  MEASURE_NAME -> measure_name\n  METRIC -> metric\n  MONTH -> month\n  YEAR -> year\n  VALUE -> value"
    )
}}
select
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION___NAME" as organisation_name,
    "MEASURE_NAME" as measure_name,
    "METRIC" as metric,
    "MONTH" as month,
    "YEAR" as year,
    "VALUE" as value
from {{ source('reference_analyst_managed', 'CYPMH_PAIRED_SCORES') }}
