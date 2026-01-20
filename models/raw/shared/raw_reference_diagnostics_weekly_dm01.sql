{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.DIAGNOSTICS_WEEKLY_DM01 \ndbt: source(''reference_analyst_managed'', ''DIAGNOSTICS_WEEKLY_DM01'') \nColumns:\n  WEEK_ENDING_DATE -> week_ending_date\n  SUBMISSION_DATE -> submission_date\n  PROVIDER_NAME -> provider_name\n  PROVIDER_SITE_NAME -> provider_site_name\n  DIAGNOSTIC_MODALITY -> diagnostic_modality\n  Waiting list_Activity -> waiting_list_activity\n  PLANNED_ACTIVITY -> planned_activity\n  UNSCHEDULED_ACTIVITY -> unscheduled_activity\n  DNA -> dna\n  PATIENT_CANCELLED -> patient_cancelled\n  Patient_Declined/Refused -> patient_declined_refused\n  0 <01 weeks -> weeks_0_lt_01\n  01 <02 weeks -> weeks_01_lt_02\n  02 <03 weeks -> weeks_02_lt_03\n  03 <04 weeks -> weeks_03_lt_04\n  04 <05 weeks -> weeks_04_lt_05\n  05 <06 weeks -> weeks_05_lt_06\n  06 <07 weeks -> weeks_06_lt_07\n  07 <08 weeks -> weeks_07_lt_08\n  08 <09 weeks -> weeks_08_lt_09\n  09 <10 weeks -> weeks_09_lt_10\n  10 <11 weeks -> weeks_10_lt_11\n  11 <12 weeks -> weeks_11_lt_12\n  12 <13 weeks -> weeks_12_lt_13\n  13+ weeks -> weeks_13_plus"
    )
}}
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
