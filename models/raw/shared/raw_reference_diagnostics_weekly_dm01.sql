-- Raw layer model for reference_analyst_managed.DIAGNOSTICS_WEEKLY_DM01
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "WEEK_ENDING_DATE" as week_ending_date,
    "SUBMISSION_DATE" as submission_date,
    "PROVIDER_NAME" as provider_name,
    "PROVIDER_SITE_NAME" as provider_site_name,
    "DIAGNOSTIC_MODALITY" as diagnostic_modality,
    "WAITING_LIST_ACTIVITY" as waiting_list_activity,
    "PLANNED_ACTIVITY" as planned_activity,
    "UNSCHEDULED_ACTIVITY" as unscheduled_activity,
    "DNA" as dna,
    "PATIENT_CANCELLED" as patient_cancelled,
    "Patient_Declined/Refused" as patient_declined_refused,
    "00 < 01 Week" as week_00_<_01,
    "01 < 02 Weeks" as weeks_01_<_02,
    "02 < 03 Weeks" as weeks_02_<_03,
    "03 < 04 Weeks" as weeks_03_<_04,
    "04 < 05 Weeks" as weeks_04_<_05,
    "05 < 06 Weeks" as weeks_05_<_06,
    "06 < 07 Weeks" as weeks_06_<_07,
    "07 < 08 Weeks" as weeks_07_<_08,
    "08 < 09 Weeks" as weeks_08_<_09,
    "09 < 10 Weeks" as weeks_09_<_10,
    "10 < 11 Weeks" as weeks_10_<_11,
    "11 < 12 Weeks" as weeks_11_<_12,
    "12 < 13 Weeks" as weeks_12_<_13,
    "13+ Weeks" as weeks_13_plus
from {{ source('reference_analyst_managed', 'DIAGNOSTICS_WEEKLY_DM01') }}
