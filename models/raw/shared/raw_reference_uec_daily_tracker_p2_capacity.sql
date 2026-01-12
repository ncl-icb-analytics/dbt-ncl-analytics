{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UEC__DAILY__TRACKER_P2_CAPACITY \ndbt: source(''reference_analyst_managed'', ''UEC__DAILY__TRACKER_P2_CAPACITY'') \nColumns:\n  DATE_DATA -> date_data\n  PROVIDER -> provider\n  UNIT -> unit\n  BEDS_AVAILABLE -> beds_available\n  BEDS_OCCUPIED -> beds_occupied\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "DATE_DATA" as date_data,
    "PROVIDER" as provider,
    "UNIT" as unit,
    "BEDS_AVAILABLE" as beds_available,
    "BEDS_OCCUPIED" as beds_occupied,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_P2_CAPACITY') }}
