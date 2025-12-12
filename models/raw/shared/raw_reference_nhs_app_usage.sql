-- Raw layer model for reference_analyst_managed.NHS_APP_USAGE
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
