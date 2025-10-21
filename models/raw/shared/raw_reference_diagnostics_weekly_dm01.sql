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
    "Waiting list_Activity" as waiting_list_activity,
    "PLANNED_ACTIVITY" as planned_activity,
    "UNSCHEDULED_ACTIVITY" as unscheduled_activity,
    "DNA" as dna,
    "PATIENT_CANCELLED" as patient_cancelled,
    "Patient_Declined/Refused" as patient_declined_refused,
    "0 <01 weeks" as weeks_0_lt_01,
    "01 <02 weeks" as weeks_01_lt_02,
    "02 <03 weeks" as weeks_02_lt_03,
    "03 <04 weeks" as weeks_03_lt_04,
    "04 <05 weeks" as weeks_04_lt_05,
    "05 <06 weeks" as weeks_05_lt_06,
    "06 <07 weeks" as weeks_06_lt_07,
    "07 <08 weeks" as weeks_07_lt_08,
    "08 <09 weeks" as weeks_08_lt_09,
    "09 <10 weeks" as weeks_09_lt_10,
    "10 <11 weeks" as weeks_10_lt_11,
    "11 <12 weeks" as weeks_11_lt_12,
    "12 <13 weeks" as weeks_12_lt_13,
    "13+ weeks" as weeks_13_plus
from {{ source('reference_analyst_managed', 'DIAGNOSTICS_WEEKLY_DM01') }}
