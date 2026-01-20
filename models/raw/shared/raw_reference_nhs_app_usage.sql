{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.NHS_APP_USAGE \ndbt: source(''reference_analyst_managed'', ''NHS_APP_USAGE'') \nColumns:\n  Date -> date\n  Logins -> logins\n  Appointments Booked -> appointments_booked\n  Appointments Cancelled -> appointments_cancelled\n  Repeat Prescriptions -> repeat_prescriptions\n  Organ Donation Registrations -> organ_donation_registrations\n  Organ Donation Withdrawals -> organ_donation_withdrawals\n  Organ Donation Updates -> organ_donation_updates\n  Organ Donation Lookup -> organ_donation_lookup\n  Record Views -> record_views\n  Summary Care Record Views -> summary_care_record_views\n  Detail Coded Record Views -> detail_coded_record_views\n  Usage Practice Code -> usage_practice_code"
    )
}}
select
    "Date" as date,
    "Logins" as logins,
    "Appointments Booked" as appointments_booked,
    "Appointments Cancelled" as appointments_cancelled,
    "Repeat Prescriptions" as repeat_prescriptions,
    "Organ Donation Registrations" as organ_donation_registrations,
    "Organ Donation Withdrawals" as organ_donation_withdrawals,
    "Organ Donation Updates" as organ_donation_updates,
    "Organ Donation Lookup" as organ_donation_lookup,
    "Record Views" as record_views,
    "Summary Care Record Views" as summary_care_record_views,
    "Detail Coded Record Views" as detail_coded_record_views,
    "Usage Practice Code" as usage_practice_code
from {{ source('reference_analyst_managed', 'NHS_APP_USAGE') }}
