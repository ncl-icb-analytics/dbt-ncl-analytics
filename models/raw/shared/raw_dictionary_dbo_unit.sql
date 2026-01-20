{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Unit \ndbt: source(''dictionary_dbo'', ''Unit'') \nColumns:\n  SK_UnitID -> sk_unit_id\n  UnitSymbol -> unit_symbol\n  UnitName -> unit_name\n  QuantityName -> quantity_name\n  SIPower -> si_power\n  IsStandardUnit -> is_standard_unit\n  IsDerivedUnit -> is_derived_unit"
    )
}}
select
    "SK_UnitID" as sk_unit_id,
    "UnitSymbol" as unit_symbol,
    "UnitName" as unit_name,
    "QuantityName" as quantity_name,
    "SIPower" as si_power,
    "IsStandardUnit" as is_standard_unit,
    "IsDerivedUnit" as is_derived_unit
from {{ source('dictionary_dbo', 'Unit') }}
