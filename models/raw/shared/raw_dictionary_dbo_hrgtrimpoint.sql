{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.HRGTrimPoint \ndbt: source(''dictionary_dbo'', ''HRGTrimPoint'') \nColumns:\n  SK_HRGID -> sk_hrgid\n  SK_TariffTypeID -> sk_tariff_type_id\n  FiscalYear -> fiscal_year\n  IsElectiveStay -> is_elective_stay\n  TrimPointDays -> trim_point_days"
    )
}}
select
    "SK_HRGID" as sk_hrgid,
    "SK_TariffTypeID" as sk_tariff_type_id,
    "FiscalYear" as fiscal_year,
    "IsElectiveStay" as is_elective_stay,
    "TrimPointDays" as trim_point_days
from {{ source('dictionary_dbo', 'HRGTrimPoint') }}
