{{
    config(
        materialized='table',
        tags=['intermediate', 'appointment', 'gp']
    )
}}

/*
Cleaned GP appointments from OLIDS

Filters to legitimate patient-facing appointments (Care Related Encounters),
resolves practitioner roles, cleans durations, and classifies contact modes
and slot categories for downstream analysis and costing.

Duration methodology:
- Use actual_duration if recorded and shorter than planned (GP finished early)
- Otherwise use planned_duration (the booked slot length)
- Cap at 60 minutes (anything above is a session/day-length data quality issue)
- Default NULLs and 0s to 10 minutes (PSSRU standard GP consultation length)

Source: PSSRU Unit Costs of Health and Social Care 2024 Manual
        https://kar.kent.ac.uk/109563/
*/

with appointments as (
    select
        a.id as appointment_id,
        a.person_id,
        a.patient_id,
        a.organisation_id,
        a.practitioner_in_role_id,
        a.schedule_id,
        a.start_date,
        a.date_time_booked,

        -- Raw durations
        a.planned_duration,
        a.actual_duration,

        -- Duration methodology: actual if reliable, else planned, then cap and default
        LEAST(
            COALESCE(
                CASE
                    WHEN a.actual_duration > 0 AND a.actual_duration < a.planned_duration
                        THEN a.actual_duration
                    WHEN a.planned_duration > 0
                        THEN a.planned_duration
                    ELSE 10
                END,
                10
            ),
            60
        ) as duration_minutes,

        -- Status
        a.appointment_status_source_code,
        a.appointment_status_source_display,
        a.appointment_status_code,
        CASE
            WHEN a.appointment_status_code = '5' THEN TRUE
            ELSE FALSE
        END as is_attended,
        CASE
            WHEN a.appointment_status_code = '3' THEN TRUE
            ELSE FALSE
        END as is_dna,

        -- Contact mode (simplified)
        a.contact_mode_source_code,
        CASE
            WHEN a.contact_mode_source_code = 'Face to Face (Surgery)' THEN 'Face-to-face'
            WHEN a.contact_mode_source_code = 'Telephone/Audio' THEN 'Telephone'
            WHEN a.contact_mode_source_code = 'Written (Including Online)' THEN 'Online'
            WHEN a.contact_mode_source_code = 'Face to Face (Home Visit)' THEN 'Home Visit'
            WHEN a.contact_mode_source_code = 'Video with Audio' THEN 'Video'
            WHEN a.contact_mode_source_code = 'Not an Appointment' THEN 'Not an Appointment'
            ELSE 'Unknown'
        END as contact_mode,

        -- Slot category
        a.national_slot_category_name,
        CASE
            WHEN a.national_slot_category_name IN (
                'General Consultation Routine', 'General Consultation Planned'
            ) THEN 'Routine'
            WHEN a.national_slot_category_name = 'General Consultation Acute'
                THEN 'Acute'
            WHEN a.national_slot_category_name IN ('Clinical Triage', 'Triage')
                THEN 'Triage'
            WHEN a.national_slot_category_name IN (
                'Planned Clinics', 'Scheduled/Planned Clinical Activity'
            ) THEN 'Planned Clinic'
            WHEN a.national_slot_category_name IN (
                'Planned Clinical Procedure', 'Scheduled/Planned Clinical Procedure'
            ) THEN 'Clinical Procedure'
            WHEN a.national_slot_category_name IN (
                'Unplanned Clinical Activity', 'Unscheduled/Unplanned Clinical Activity'
            ) THEN 'Unplanned'
            WHEN a.national_slot_category_name IN (
                'Home Visit', 'Care Home Visit',
                'Home Visit and Care Home Visit',
                'Patient contact during Care Home Round'
            ) THEN 'Home/Care Home Visit'
            WHEN a.national_slot_category_name = 'Structured Medication Review'
                THEN 'Medication Review'
            WHEN a.national_slot_category_name = 'Walk-in'
                THEN 'Walk-in'
            WHEN a.national_slot_category_name = 'Social Prescribing Service'
                THEN 'Social Prescribing'
            WHEN a.national_slot_category_name IN (
                'Care Home Needs Assessment & Personalised Care and Support Planning'
            ) THEN 'Care Home Assessment'
            WHEN a.national_slot_category_name = 'Non-contractual chargeable work'
                THEN 'Non-NHS Chargeable'
            WHEN a.national_slot_category_name = 'Service provided by organisation external to the practice'
                THEN 'External Service'
            WHEN a.national_slot_category_name = 'Group Consultation and Group Education'
                THEN 'Group Consultation'
            ELSE 'Other'
        END as slot_category,

        -- Urgency classification (NHSE GPAD methodology)
        CASE
            WHEN a.national_slot_category_name IN (
                'General Consultation Acute', 'Unplanned Clinical Activity',
                'Unscheduled/Unplanned Clinical Activity', 'Walk-in',
                'Clinical Triage', 'Triage'
            ) THEN 'Urgent'
            WHEN a.national_slot_category_name IN (
                'General Consultation Routine', 'General Consultation Planned',
                'Planned Clinics', 'Planned Clinical Procedure',
                'Scheduled/Planned Clinical Procedure',
                'Scheduled/Planned Clinical Activity',
                'Structured Medication Review'
            ) THEN 'Routine'
            ELSE 'Other'
        END as urgency,
        -- Same-day: booked and seen on the same day (NHSE standard definition)
        CASE
            WHEN a.date_time_booked IS NOT NULL
                AND DATE(a.date_time_booked) = DATE(a.start_date)
                THEN TRUE
            ELSE FALSE
        END as is_same_day,
        -- Days between booking and appointment
        CASE
            WHEN a.date_time_booked IS NOT NULL
                THEN DATEDIFF('day', DATE(a.date_time_booked), DATE(a.start_date))
        END as days_to_appointment,

        -- Patient experience
        a.patient_wait,
        a.patient_delay,

        -- Booking
        a.booking_method_source_code as booking_method,

        -- Context
        a.type as local_slot_type,
        a.context_type,
        a.service_setting,
        a.age_at_event,
        a.record_owner_organisation_code

    from {{ ref('stg_olids_appointment') }} as a
    where a.context_type = 'Care Related Encounter'
      and a.appointment_status_code != '0'  -- exclude future/available slots
),

practitioner_roles as (
    select
        pir.id as practitioner_in_role_id,
        pir.practitioner_id,
        pir.organisation_id as practitioner_org_id,
        pir.role_code,
        pir.role as role_name,
        CASE
            WHEN pir.role_code IN ('R0260', 'R0270', 'R6300', 'R0262', 'R6200')
                THEN 'GP'
            WHEN pir.role_code IN ('R0690', 'R0700', 'R0620', 'R0600', 'R0570',
                                    'R0580', 'R1543', 'R0630')
                THEN 'Nurse'
            WHEN pir.role_code IN ('R1290', 'R9804')
                THEN 'Pharmacist'
            WHEN pir.role_code IN ('R1480', 'R1450')
                THEN 'HCA'
            WHEN pir.role_code IN ('R1547', 'E1003', 'R9813')
                THEN 'Physician Associate'
            WHEN pir.role_code IN ('R1370')
                THEN 'Other Clinical'
            WHEN pir.role_code IN ('R1730', 'R1720', 'R1982', 'R1973', 'R1760',
                                    'R1780', 'R1790', 'R1800')
                THEN 'Admin'
            ELSE 'Other'
        END as practitioner_role_group
    from {{ ref('stg_olids_practitioner_in_role') }} as pir
)

select
    a.appointment_id,
    a.person_id,
    a.patient_id,
    a.organisation_id,
    a.start_date,
    a.date_time_booked,

    -- Duration
    a.planned_duration,
    a.actual_duration,
    a.duration_minutes,

    -- Status
    a.appointment_status_source_code,
    a.appointment_status_source_display,
    a.is_attended,
    a.is_dna,

    -- Contact mode
    a.contact_mode_source_code,
    a.contact_mode,

    -- Slot category
    a.national_slot_category_name,
    a.slot_category,

    -- Practitioner
    pr.practitioner_id,
    pr.role_code,
    pr.role_name,
    pr.practitioner_role_group,

    -- Urgency
    a.urgency,
    a.is_same_day,
    a.days_to_appointment,

    -- Patient experience
    a.patient_wait,
    a.patient_delay,

    -- Booking and context
    a.booking_method,
    a.local_slot_type,
    a.service_setting,
    a.age_at_event,
    a.record_owner_organisation_code

from appointments as a
left join practitioner_roles as pr
    on a.practitioner_in_role_id = pr.practitioner_in_role_id
