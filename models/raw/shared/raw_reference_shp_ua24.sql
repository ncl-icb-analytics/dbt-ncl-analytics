{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.SHP_UA24 \ndbt: source(''reference_analyst_managed'', ''SHP_UA24'') \nColumns:\n  GEOMETRY -> geometry\n  PROPERTIES -> properties\n  TYPE -> type"
    )
}}
select
    "GEOMETRY" as geometry,
    "PROPERTIES" as properties,
    "TYPE" as type
from {{ source('reference_analyst_managed', 'SHP_UA24') }}
