-- Staging model for dictionary_dbo.HRGTrimPoint_Pivoted
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_HRGID" as sk_hrgid,
    "TariffType" as tarifftype,
    "TariffTypeDesc" as tarifftypedesc,
    "FiscalYear" as fiscalyear,
    "Elective_TrimPointDays" as elective_trimpointdays,
    "Non-Elective_TrimPointDays" as non_elective_trimpointdays
from {{ source('dictionary_dbo', 'HRGTrimPoint_Pivoted') }}
