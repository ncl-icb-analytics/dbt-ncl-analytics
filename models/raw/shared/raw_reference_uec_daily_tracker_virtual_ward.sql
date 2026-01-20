{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UEC__DAILY__TRACKER_VIRTUAL_WARD \ndbt: source(''reference_analyst_managed'', ''UEC__DAILY__TRACKER_VIRTUAL_WARD'') \nColumns:\n  DATE_DATA -> date_data\n  CAPACITY -> capacity\n  OCCUPIED -> occupied\n  SYSTEM_VALUE -> system_value\n  IS_INCLUDES_PAEDIATRIC -> is_includes_paediatric\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "DATE_DATA" as date_data,
    "CAPACITY" as capacity,
    "OCCUPIED" as occupied,
    "SYSTEM_VALUE" as system_value,
    "IS_INCLUDES_PAEDIATRIC" as is_includes_paediatric,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_VIRTUAL_WARD') }}
