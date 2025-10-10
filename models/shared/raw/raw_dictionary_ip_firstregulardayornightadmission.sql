-- Raw layer model for dictionary_ip.FirstRegularDayOrNightAdmission
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_FirstRegularDayOrNightAdmissionID" as sk_first_regular_day_or_night_admission_id,
    "BK_FirstRegularDayOrNightAdmission" as bk_first_regular_day_or_night_admission,
    "FirstRegularDayOrNightAdmission" as first_regular_day_or_night_admission
from {{ source('dictionary_ip', 'FirstRegularDayOrNightAdmission') }}
