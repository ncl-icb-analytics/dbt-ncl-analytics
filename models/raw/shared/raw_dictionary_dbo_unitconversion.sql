{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.UnitConversion \ndbt: source(''dictionary_dbo'', ''UnitConversion'') \nColumns:\n  SK_UnitID_Source -> sk_unit_id_source\n  SK_UnitID_Target -> sk_unit_id_target\n  Subtrahend -> subtrahend\n  Multiplier -> multiplier\n  Divisor -> divisor\n  Addend -> addend"
    )
}}
select
    "SK_UnitID_Source" as sk_unit_id_source,
    "SK_UnitID_Target" as sk_unit_id_target,
    "Subtrahend" as subtrahend,
    "Multiplier" as multiplier,
    "Divisor" as divisor,
    "Addend" as addend
from {{ source('dictionary_dbo', 'UnitConversion') }}
