{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MODEL_HOSPITAL_METRIC_MAPPING \ndbt: source(''reference_analyst_managed'', ''MODEL_HOSPITAL_METRIC_MAPPING'') \nColumns:\n  Metric -> metric\n  Metric_Mapping -> metric_mapping"
    )
}}
select
    "Metric" as metric,
    "Metric_Mapping" as metric_mapping
from {{ source('reference_analyst_managed', 'MODEL_HOSPITAL_METRIC_MAPPING') }}
