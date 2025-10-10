-- Raw layer model for dictionary_dbo.HRGTrimPoint
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_HRGID" as sk_hrgid,
    "SK_TariffTypeID" as sk_tariff_type_id,
    "FiscalYear" as fiscal_year,
    "IsElectiveStay" as is_elective_stay,
    "TrimPointDays" as trim_point_days
from {{ source('dictionary_dbo', 'HRGTrimPoint') }}
