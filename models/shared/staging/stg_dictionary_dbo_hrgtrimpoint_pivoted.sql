-- Staging model for dictionary_dbo.HRGTrimPoint_Pivoted
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_HRGID" as sk_hrgid,
    "TariffType" as tariff_type,
    "TariffTypeDesc" as tariff_type_desc,
    "FiscalYear" as fiscal_year,
    "Elective_TrimPointDays" as elective_trim_point_days,
    "Non-Elective_TrimPointDays" as non_elective_trim_point_days
from {{ source('dictionary_dbo', 'HRGTrimPoint_Pivoted') }}
