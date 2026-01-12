{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.UnitMapping \ndbt: source(''dictionary_dbo'', ''UnitMapping'') \nColumns:\n  SK_UnitID -> sk_unit_id\n  UnitLabel -> unit_label"
    )
}}
select
    "SK_UnitID" as sk_unit_id,
    "UnitLabel" as unit_label
from {{ source('dictionary_dbo', 'UnitMapping') }}
