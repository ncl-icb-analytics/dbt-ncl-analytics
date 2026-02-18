{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.NHS_APP \ndbt: source(''reference_analyst_managed'', ''NHS_APP'') \nColumns:\n  DATE -> date\n  LOGINS -> logins\n  APPOINTMENTS_BOOKED -> appointments_booked\n  APPOINTMENTS_CANCELLED -> appointments_cancelled\n  REPEAT_PRESCRIPTIONS -> repeat_prescriptions\n  ORGAN_DONATION_REGISTRATIONS -> organ_donation_registrations\n  ORGAN_DONATION_WITHDRAWALS -> organ_donation_withdrawals\n  ORGAN_DONATION_UPDATES -> organ_donation_updates\n  ORGAN_DONATION_LOOKUP -> organ_donation_lookup\n  RECORD_VIEWS -> record_views\n  SUMMARY_CARE_RECORD_VIEWS -> summary_care_record_views\n  DETAIL_CODED_RECORD_VIEWS -> detail_coded_record_views\n  USAGE_PRACTICE_CODE -> usage_practice_code"
    )
}}
select
    "DATE" as date,
    "LOGINS" as logins,
    "APPOINTMENTS_BOOKED" as appointments_booked,
    "APPOINTMENTS_CANCELLED" as appointments_cancelled,
    "REPEAT_PRESCRIPTIONS" as repeat_prescriptions,
    "ORGAN_DONATION_REGISTRATIONS" as organ_donation_registrations,
    "ORGAN_DONATION_WITHDRAWALS" as organ_donation_withdrawals,
    "ORGAN_DONATION_UPDATES" as organ_donation_updates,
    "ORGAN_DONATION_LOOKUP" as organ_donation_lookup,
    "RECORD_VIEWS" as record_views,
    "SUMMARY_CARE_RECORD_VIEWS" as summary_care_record_views,
    "DETAIL_CODED_RECORD_VIEWS" as detail_coded_record_views,
    "USAGE_PRACTICE_CODE" as usage_practice_code
from {{ source('reference_analyst_managed', 'NHS_APP') }}
