{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS GP Appointments Semantic View
    ====================================

    Self-contained appointment-level semantic model combining GP access data
    with patient demographics, conditions, practice details, and PSSRU unit costs.
    OLIDS is the One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the One London team.

    Grain: One row per appointment

    Use Cases:
    - GP contract access KPIs (urgent same-day, routine 7/14 day)
    - DNA rates by deprivation, ethnicity, age band
    - Contact mode trends (F2F vs telephone vs online)
    - Workforce mix analysis (GP vs nurse vs pharmacist)
    - Appointment costing using PSSRU unit costs
    - Equity analysis: access by IMD, borough, condition
    - Practice-level access comparison

    Practice Attribution:
    - practice_code / practice_name / pcn_name / borough come from dim_practice
      joined via the appointment's record_owner_organisation_code — this is the
      practice that delivered the appointment.
    - registered_* columns come from the patient's CURRENT registration in
      dim_person_demographics and may differ (e.g. patient transferred since
      the appointment, or attended a different practice via NHS-wide access).
#}

TABLES(
    appt AS {{ ref('int_appointment_gp_clean_recent') }}
        PRIMARY KEY (appointment_id)
        COMMENT = 'Cleaned GP appointments — Care Related Encounters only, restricted to last 60 months matching OLIDS retention',
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',
    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Long-term condition flags and diabetes type classification',
    practice AS {{ ref('dim_practice') }}
        PRIMARY KEY (practice_code)
        COMMENT = 'Practice details for the appointment-owning practice (name, PCN, borough)',
    costs AS {{ ref('pssru_unit_costs_2024') }}
        PRIMARY KEY (practitioner_role_group)
        COMMENT = 'PSSRU unit costs per practitioner role group (2023/2024 prices)'
)

RELATIONSHIPS(
    appt (person_id) REFERENCES demographics,
    appt (person_id) REFERENCES conditions,
    appt (record_owner_organisation_code) REFERENCES practice (practice_code),
    appt (practitioner_role_group) REFERENCES costs
)

FACTS(
    appt.duration_minutes AS duration_minutes COMMENT = 'Cleaned appointment duration in minutes',
    appt.planned_duration AS planned_duration COMMENT = 'Original planned slot duration in minutes',
    appt.booking_to_slot_days AS booking_to_slot_days COMMENT = 'Calendar days from booking to the appointment slot time (0 = same day)',
    appt.patient_wait AS patient_wait COMMENT = 'Minutes patient waited beyond scheduled time',
    appt.patient_delay AS patient_delay COMMENT = 'Minutes patient arrived late',
    appt.age_at_event AS age_at_event COMMENT = 'Patient age at appointment (event-time, stable for historical analysis)',
    demographics.age AS age COMMENT = 'Patient current age (drifts over time — use age_at_event for historical cohorting)',
    conditions.total_conditions AS total_conditions COMMENT = 'Total active long-term conditions',
    appt.pssru_cost_per_minute_gbp AS pssru_cost_per_minute_gbp COMMENT = 'PSSRU 2024 cost per minute for the appointment practitioner role group (2023/24 prices)',
    appt.appointment_cost_gbp_base_prices AS appointment_cost_gbp_base_prices COMMENT = 'Appointment cost in PSSRU base year prices (2023/24) — real-terms. Use for cross-year comparisons.',
    appt.appointment_cost_gbp_nominal AS appointment_cost_gbp_nominal COMMENT = 'Appointment cost in contemporaneous fiscal year prices (GDP deflator adjusted from PSSRU 2023/24 base). NULL for fiscal years outside uk_cost_indices seed coverage (pre 2000-01).',
    costs.cost_per_minute_gbp AS cost_per_minute_gbp COMMENT = 'Legacy: PSSRU cost per minute for this role group (same as pssru_cost_per_minute_gbp on appt — retained for back-compatibility)'
)

DIMENSIONS(
    -- Appointment time
    appt.start_date AS start_date WITH SYNONYMS = ('appointment date', 'date') COMMENT = 'Appointment date and time',

    -- Appointment status
    appt.is_attended AS is_attended COMMENT = 'TRUE if patient attended',
    appt.is_dna AS is_dna WITH SYNONYMS = ('did not attend', 'no show') COMMENT = 'TRUE if did not attend',

    -- Urgency and access
    appt.urgency AS urgency WITH SYNONYMS = ('urgent', 'routine') COMMENT = 'Appointment urgency (Urgent, Routine, Other). Only General Consultation Acute maps to Urgent per NHSE GP contract 2026/27.',
    appt.is_same_day AS is_same_day WITH SYNONYMS = ('same day') COMMENT = 'Booked and seen same day',

    -- Contact mode
    appt.contact_mode AS contact_mode WITH SYNONYMS = ('mode', 'delivery mode') COMMENT = 'Contact mode (Face-to-face, Telephone, Online, Home Visit, Video, Unknown)',
    appt.contact_mode_source_code AS contact_mode_source_code COMMENT = 'Raw contact mode code from source system',

    -- Slot category
    appt.slot_category AS slot_category WITH SYNONYMS = ('appointment type', 'category') COMMENT = 'Simplified slot category (Routine, Acute, Triage, Planned Clinic, Clinical Procedure, Unplanned, Home/Care Home Visit, Medication Review, Walk-in, Social Prescribing, Care Home Assessment, Non-NHS Chargeable, External Service, Group Consultation, Other)',
    appt.national_slot_category_name AS national_slot_category_name COMMENT = 'Raw national slot category name from source system',

    -- Practitioner
    appt.practitioner_role_group AS practitioner_role_group WITH SYNONYMS = ('HCP type', 'staff type', 'role') COMMENT = 'Analytical role grouping (GP, Nurse, Pharmacist, HCA, Physician Associate, Paramedic, Physiotherapist, Care Navigator, Counsellor, Mental Health Practitioner, Health & Wellbeing Coach, Social Prescriber, Dietitian, Podiatrist, Occupational Therapist, Other Direct Patient Care, Admin/Non-Clinical, Unknown)',
    appt.sds_role_group AS sds_role_group WITH SYNONYMS = ('SDS group', 'NHS role group') COMMENT = 'Official NHS Digital SDS role group (GP, Nurses, Other Direct Patient Care, Admin/Data Quality, Unknown) — aligns with national GPAD publications',
    appt.role_name AS role_name COMMENT = 'Raw practitioner role name as recorded by the practice',
    appt.practitioner_name AS practitioner_name COMMENT = 'Clinician full name — personal data, restrict access accordingly',
    appt.is_arrs_role AS is_arrs_role WITH SYNONYMS = ('ARRS', 'additional roles') COMMENT = 'TRUE where the SDS code unambiguously identifies an ARRS-scheme role',
    appt.schedule_type AS schedule_type COMMENT = 'Raw schedule type from OLIDS',
    appt.is_untimed_session AS is_untimed_session COMMENT = 'TRUE if parent schedule is an open/untimed session (duty doctor, eConsult list). Duration is NULL for these.',

    -- Appointment-owning practice (the practice that delivered the appointment)
    appt.practice_code AS record_owner_organisation_code WITH SYNONYMS = ('practice code', 'GP practice', 'ODS code') COMMENT = 'ODS code of the practice that owns this appointment',
    practice.practice_name AS practice_name COMMENT = 'Name of the practice that owns this appointment',
    practice.pcn_code AS pcn_code COMMENT = 'PCN code of the appointment-owning practice',
    practice.pcn_name AS pcn_name COMMENT = 'PCN name of the appointment-owning practice',
    practice.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN with borough prefix for the appointment-owning practice',
    practice.borough_registered AS borough_registered COMMENT = 'Borough of the appointment-owning practice',

    -- Booking
    appt.booking_method AS booking_method COMMENT = 'Booking method source code',

    -- Patient demographics (current snapshot — slowly changing attributes
    -- like ethnicity/language are reasonable for historical analysis;
    -- age-derived bands are CURRENT and drift over time)
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = 'Current 5-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.age_band_10y AS age_band_10y COMMENT = 'Current 10-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'Current NHS standard age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'Current ESP 2013 age band (drifts — use age_at_event for historical). (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+)',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Current life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown). Drifts — use age_at_event for historical.',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',

    -- Patient registration status
    demographics.is_active AS is_active COMMENT = 'Patient currently registered with an NCL GP practice',

    -- Geography (patient residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Patient LSOA 2021 code (residence-based)',
    demographics.ward_code AS ward_code COMMENT = 'Patient electoral ward 2025 code (residence-based)',
    demographics.ward_name AS ward_name COMMENT = 'Patient electoral ward 2025 name (residence-based)',
    demographics.borough_resident AS borough_resident COMMENT = 'Patient borough of residence',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Patient NCL neighbourhood of residence',

    -- Deprivation (patient residence)
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Key conditions (for equity/utilisation analysis — current state, not at appointment time)
    conditions.diabetes_type AS diabetes_type COMMENT = 'Diabetes type (Type 1, Type 2, Unknown, Not Diabetic)',
    conditions.has_diabetes AS has_diabetes WITH SYNONYMS = ('DM', 'diabetic') COMMENT = 'On diabetes register',
    conditions.has_hypertension AS has_hypertension WITH SYNONYMS = ('HTN') COMMENT = 'On hypertension register',
    conditions.has_copd AS has_copd COMMENT = 'On COPD register',
    conditions.has_asthma AS has_asthma COMMENT = 'On asthma register',
    conditions.has_depression AS has_depression COMMENT = 'On depression register',
    conditions.has_severe_mental_illness AS has_severe_mental_illness WITH SYNONYMS = ('SMI') COMMENT = 'On SMI register',
    conditions.has_dementia AS has_dementia COMMENT = 'On dementia register',
    conditions.has_heart_failure AS has_heart_failure WITH SYNONYMS = ('HF') COMMENT = 'On heart failure register',
    conditions.has_chronic_kidney_disease AS has_chronic_kidney_disease WITH SYNONYMS = ('CKD') COMMENT = 'On CKD register',
    conditions.has_cancer AS has_cancer COMMENT = 'On cancer register',
    conditions.has_frailty AS has_frailty COMMENT = 'Recorded frailty',
    conditions.has_learning_disability AS has_learning_disability WITH SYNONYMS = ('LD') COMMENT = 'On LD register',

    -- Cost reference
    costs.afc_band AS afc_band COMMENT = 'Agenda for Change band for role group'
)

METRICS(
    -- Volume
    appt.appointment_count AS COUNT(appt.appointment_id) COMMENT = 'Total appointments',
    appt.attended_count AS COUNT(CASE WHEN appt.is_attended THEN appt.appointment_id END) COMMENT = 'Attended appointments',
    appt.dna_count AS COUNT(CASE WHEN appt.is_dna THEN appt.appointment_id END) COMMENT = 'DNA appointments',
    appt.patient_count AS COUNT(DISTINCT appt.person_id) COMMENT = 'Distinct patients',

    -- DNA rate
    appt.dna_rate AS COUNT(CASE WHEN appt.is_dna THEN appt.appointment_id END) / NULLIF(COUNT(appt.appointment_id), 0) COMMENT = 'DNA rate (0-1)',

    -- Access KPIs
    appt.urgent_same_day_count AS COUNT(CASE WHEN appt.urgency = 'Urgent' AND appt.is_attended AND appt.is_same_day THEN appt.appointment_id END) COMMENT = 'Urgent seen same day',
    appt.urgent_attended_count AS COUNT(CASE WHEN appt.urgency = 'Urgent' AND appt.is_attended THEN appt.appointment_id END) COMMENT = 'All urgent attended',
    appt.routine_within_7d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.booking_to_slot_days <= 7 THEN appt.appointment_id END) COMMENT = 'Routine within 7 days',
    appt.routine_within_14d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.booking_to_slot_days <= 14 THEN appt.appointment_id END) COMMENT = 'Routine within 14 days',
    appt.routine_attended_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended THEN appt.appointment_id END) COMMENT = 'All routine attended',

    -- Duration and wait
    appt.avg_duration AS AVG(appt.duration_minutes) COMMENT = 'Average duration (minutes)',
    appt.total_duration AS SUM(appt.duration_minutes) COMMENT = 'Total appointment minutes',
    appt.avg_booking_to_slot_days AS AVG(appt.booking_to_slot_days) COMMENT = 'Average days from booking to the appointment slot',
    appt.avg_patient_wait AS AVG(appt.patient_wait) COMMENT = 'Average wait beyond scheduled time (minutes)'
)

COMMENT = 'OLIDS GP Appointments with practice details, patient demographics, conditions, and PSSRU costs. Grain: one row per appointment. practice_code/practice_name = appointment-owning practice; registered_* = patient current registration (may differ). Supports GP contract access KPIs, DNA equity analysis, workforce mix, and utilisation by condition/deprivation.'
AI_SQL_GENERATION 'For GP contract KPIs: urgent_same_day_count / urgent_attended_count = same-day rate; routine_within_7d_count / routine_attended_count = 7-day rate. For practice-level analysis, group by practice_code or practice_name (appointment owner). For patient registration analysis, use registered_practice_name or registered_borough. For equity analysis, group by imd_quintile_25 or ethnicity_category. For condition-specific utilisation, filter on has_diabetes etc. Group by DATE_TRUNC(month, start_date) for trends. Cost estimation: aggregate the per-appointment cost facts directly — SUM(appointment_cost_gbp_base_prices) for real-terms cost in PSSRU 2023/24 prices (use for cross-year comparisons), or SUM(appointment_cost_gbp_nominal) for contemporaneous (GDP-deflator-adjusted) cost. Do NOT derive cost from total_duration * cost_per_minute_gbp — that ignores the per-row deflator adjustment.'
AI_QUESTION_CATEGORIZATION 'Use this view for: GP appointment access, same-day urgent access, wait times, DNA rates by deprivation/ethnicity, contact mode trends, workforce mix, utilisation by condition, practice-level access comparison, and GP contract KPIs. For current population snapshots without appointment data use sem_olids_population. For clinical biomarkers use sem_olids_observations. For time-series condition trends use sem_olids_trends.'
