{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.FirstRegularDayOrNightAdmission \ndbt: source(''dictionary_ip'', ''FirstRegularDayOrNightAdmission'') \nColumns:\n  SK_FirstRegularDayOrNightAdmissionID -> sk_first_regular_day_or_night_admission_id\n  BK_FirstRegularDayOrNightAdmission -> bk_first_regular_day_or_night_admission\n  FirstRegularDayOrNightAdmission -> first_regular_day_or_night_admission"
    )
}}
select
    "SK_FirstRegularDayOrNightAdmissionID" as sk_first_regular_day_or_night_admission_id,
    "BK_FirstRegularDayOrNightAdmission" as bk_first_regular_day_or_night_admission,
    "FirstRegularDayOrNightAdmission" as first_regular_day_or_night_admission
from {{ source('dictionary_ip', 'FirstRegularDayOrNightAdmission') }}
