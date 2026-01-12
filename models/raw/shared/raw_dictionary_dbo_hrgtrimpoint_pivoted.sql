{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.HRGTrimPoint_Pivoted \ndbt: source(''dictionary_dbo'', ''HRGTrimPoint_Pivoted'') \nColumns:\n  SK_HRGID -> sk_hrgid\n  TariffType -> tariff_type\n  TariffTypeDesc -> tariff_type_desc\n  FiscalYear -> fiscal_year\n  Elective_TrimPointDays -> elective_trim_point_days\n  Non-Elective_TrimPointDays -> non_elective_trim_point_days"
    )
}}
select
    "SK_HRGID" as sk_hrgid,
    "TariffType" as tariff_type,
    "TariffTypeDesc" as tariff_type_desc,
    "FiscalYear" as fiscal_year,
    "Elective_TrimPointDays" as elective_trim_point_days,
    "Non-Elective_TrimPointDays" as non_elective_trim_point_days
from {{ source('dictionary_dbo', 'HRGTrimPoint_Pivoted') }}
