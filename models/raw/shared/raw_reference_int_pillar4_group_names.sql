{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.INT_PILLAR4_GROUP_NAMES \ndbt: source(''reference_analyst_managed'', ''INT_PILLAR4_GROUP_NAMES'') \nColumns:\n  P4_GROUP_KEY -> p4_group_key\n  P4_GROUP_NAME -> p4_group_name\n  LTC_COUNT -> ltc_count"
    )
}}
select
    "P4_GROUP_KEY" as p4_group_key,
    "P4_GROUP_NAME" as p4_group_name,
    "LTC_COUNT" as ltc_count
from {{ source('reference_analyst_managed', 'INT_PILLAR4_GROUP_NAMES') }}
