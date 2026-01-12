{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.DIAGNOSTICS_WEEKLY_DM01_HISTORIC \ndbt: source(''reference_analyst_managed'', ''DIAGNOSTICS_WEEKLY_DM01_HISTORIC'') \nColumns:\n  PROVIDER -> provider\n  SPECIALITY -> speciality\n  DATE -> date\n  WEEKNUMBER -> weeknumber\n  Diagnostic waiting list-With a TCI -> diagnostic_waiting_list_with_a_tci\n  Diagnostic waiting list-Without a TCI -> diagnostic_waiting_list_without_a_tci\n  Diagnostic waiting list-Total -> diagnostic_waiting_list_total\n  Waiting list -> waiting_list\n  PLANNED -> planned\n  UNSCHEDULED -> unscheduled\n  DM01-Total -> dm01_total\n  DNA -> dna\n  Patient Cancelled -> patient_cancelled\n  Patient Declined/Refused -> patient_declined_refused\n  Total DNA -> total_dna\n  Cancer 62day pathway - Total on waiting list -> cancer_62day_pathway_total_on_waiting_list\n  Cancer 62 day pathway - Activity in the week -> cancer_62_day_pathway_activity_in_the_week\n  Wait List-0<01 weeks -> wait_list_0_lt_01_weeks\n  Wait List-01<02 weeks -> wait_list_01_lt_02_weeks\n  Wait List-02<03 weeks -> wait_list_02_lt_03_weeks\n  Wait List-03<04 weeks -> wait_list_03_lt_04_weeks\n  Wait List-04<05 weeks -> wait_list_04_lt_05_weeks\n  Wait List-05<06 weeks -> wait_list_05_lt_06_weeks"
    )
}}
select
    "PROVIDER" as provider,
    "SPECIALITY" as speciality,
    "DATE" as date,
    "WEEKNUMBER" as weeknumber,
    "Diagnostic waiting list-With a TCI" as diagnostic_waiting_list_with_a_tci,
    "Diagnostic waiting list-Without a TCI" as diagnostic_waiting_list_without_a_tci,
    "Diagnostic waiting list-Total" as diagnostic_waiting_list_total,
    "Waiting list" as waiting_list,
    "PLANNED" as planned,
    "UNSCHEDULED" as unscheduled,
    "DM01-Total" as dm01_total,
    "DNA" as dna,
    "Patient Cancelled" as patient_cancelled,
    "Patient Declined/Refused" as patient_declined_refused,
    "Total DNA" as total_dna,
    "Cancer 62day pathway - Total on waiting list" as cancer_62day_pathway_total_on_waiting_list,
    "Cancer 62 day pathway - Activity in the week" as cancer_62_day_pathway_activity_in_the_week,
    "Wait List-0<01 weeks" as wait_list_0_lt_01_weeks,
    "Wait List-01<02 weeks" as wait_list_01_lt_02_weeks,
    "Wait List-02<03 weeks" as wait_list_02_lt_03_weeks,
    "Wait List-03<04 weeks" as wait_list_03_lt_04_weeks,
    "Wait List-04<05 weeks" as wait_list_04_lt_05_weeks,
    "Wait List-05<06 weeks" as wait_list_05_lt_06_weeks
from {{ source('reference_analyst_managed', 'DIAGNOSTICS_WEEKLY_DM01_HISTORIC') }}
