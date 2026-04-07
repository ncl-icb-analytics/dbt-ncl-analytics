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
    with patient demographics, conditions, and PSSRU unit costs.

    Grain: One row per appointment

    Use Cases:
    - GP contract access KPIs (urgent same-day, routine 7/14 day)
    - DNA rates by deprivation, ethnicity, age band
    - Contact mode trends (F2F vs telephone vs online)
    - Workforce mix analysis (GP vs nurse vs pharmacist)
    - Appointment costing using PSSRU unit costs
    - Equity analysis: access by IMD, borough, condition
#}

TABLES(
    appt AS {{ ref('int_appointment_gp_clean') }}
        PRIMARY KEY (appointment_id)
        COMMENT = 'Cleaned GP appointments - Care Related Encounters only',
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation',
    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Long-term condition flags',
    costs AS {{ ref('pssru_unit_costs_2024') }}
        PRIMARY KEY (practitioner_role_group)
        COMMENT = 'PSSRU unit costs per practitioner role group (2023/2024 prices)'
)

RELATIONSHIPS(
    appt (person_id) REFERENCES demographics,
    appt (person_id) REFERENCES conditions,
    appt (practitioner_role_group) REFERENCES costs
)

FACTS(
    appt.duration_minutes AS duration_minutes COMMENT = 'Cleaned appointment duration in minutes',
    appt.planned_duration AS planned_duration COMMENT = 'Original planned slot duration in minutes',
    appt.days_to_appointment AS days_to_appointment COMMENT = 'Days between booking and appointment',
    appt.patient_wait AS patient_wait COMMENT = 'Minutes patient waited beyond scheduled time',
    appt.patient_delay AS patient_delay COMMENT = 'Minutes patient arrived late',
    appt.age_at_event AS age_at_event COMMENT = 'Patient age at appointment (event-time, stable for historical analysis)',
    demographics.current_age AS age COMMENT = 'Patient current age (drifts over time — do not use to cohort historical appointments)',
    conditions.total_conditions AS total_conditions COMMENT = 'Total active long-term conditions',
    costs.cost_per_minute_gbp AS cost_per_minute_gbp COMMENT = 'PSSRU cost per minute for this role group'
)

DIMENSIONS(
    -- Appointment time
    appt.start_date AS start_date WITH SYNONYMS = ('appointment date', 'date') COMMENT = 'Appointment date and time',

    -- Appointment status
    appt.is_attended AS is_attended COMMENT = 'TRUE if patient attended',
    appt.is_dna AS is_dna WITH SYNONYMS = ('did not attend', 'no show') COMMENT = 'TRUE if did not attend',

    -- Urgency and access
    appt.urgency AS urgency WITH SYNONYMS = ('urgent', 'routine') COMMENT = 'Urgent, Routine, or Other',
    appt.is_same_day AS is_same_day WITH SYNONYMS = ('same day') COMMENT = 'Booked and seen same day',

    -- Contact mode
    appt.contact_mode AS contact_mode WITH SYNONYMS = ('mode', 'delivery mode') COMMENT = 'Face-to-face, Telephone, Online, Home Visit, Video',
    appt.contact_mode_source_code AS contact_mode_source_code COMMENT = 'Raw contact mode',

    -- Slot category
    appt.slot_category AS slot_category WITH SYNONYMS = ('appointment type', 'category') COMMENT = 'Simplified national slot category',
    appt.national_slot_category_name AS national_slot_category_name COMMENT = 'Raw national slot category',

    -- Practitioner
    appt.practitioner_role_group AS practitioner_role_group WITH SYNONYMS = ('HCP type', 'staff type', 'role') COMMENT = 'GP, Nurse, Pharmacist, HCA, Physician Associate, Admin, Other',
    appt.role_name AS role_name COMMENT = 'Detailed practitioner role name',
    appt.is_arrs_role AS is_arrs_role WITH SYNONYMS = ('ARRS', 'additional roles') COMMENT = 'TRUE if ARRS-funded role (pharmacist, physio, paramedic, PA, care navigator, counsellor)',
    appt.schedule_type AS schedule_type COMMENT = 'Raw schedule type from OLIDS',
    appt.is_untimed_session AS is_untimed_session COMMENT = 'TRUE if parent schedule is an open/untimed session (duty doctor, eConsult list)',

    -- Organisation
    appt.record_owner_organisation_code AS record_owner_organisation_code WITH SYNONYMS = ('practice code', 'GP practice') COMMENT = 'Practice ODS code',

    -- Booking
    appt.booking_method AS booking_method COMMENT = 'Booking method',

    -- Patient demographics (current snapshot — slowly changing attributes
    -- like ethnicity/language are reasonable for historical analysis;
    -- age-derived bands are CURRENT and drift over time, so are prefixed
    -- current_ to make event-time vs current intent explicit)
    demographics.gender AS gender COMMENT = 'Patient gender',
    demographics.current_age_band_5y AS age_band_5y COMMENT = 'Current 5-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.current_age_band_10y AS age_band_10y COMMENT = 'Current 10-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.current_age_life_stage AS age_life_stage COMMENT = 'Current life stage (drifts — use age_at_event for cohorting historical appointments)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    demographics.main_language AS main_language COMMENT = 'Main spoken language',

    -- Current registration (NOT appointment-owner — for appointment-owner
    -- practice attribution use record_owner_organisation_code on appt)
    demographics.is_active AS is_active COMMENT = 'Currently registered',
    demographics.current_practice_name AS practice_name COMMENT = 'Current registered practice name (NOT appointment owner — use record_owner_organisation_code for appointment-owner attribution)',
    demographics.current_pcn_name AS pcn_name COMMENT = 'Current registered PCN name (NOT appointment owner)',
    demographics.current_borough_registered AS borough_registered COMMENT = 'Borough of current registered practice (NOT appointment owner)',
    demographics.current_neighbourhood_registered AS neighbourhood_registered COMMENT = 'Neighbourhood of current registration (NOT appointment owner)',

    -- Geography (residence)
    demographics.borough_resident AS borough_resident COMMENT = 'Borough of residence',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward name',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Neighbourhood of residence',

    -- Deprivation
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived)',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile',

    -- Key conditions (for equity/utilisation analysis)
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
    appt.routine_within_7d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.days_to_appointment <= 7 THEN appt.appointment_id END) COMMENT = 'Routine within 7 days',
    appt.routine_within_14d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.days_to_appointment <= 14 THEN appt.appointment_id END) COMMENT = 'Routine within 14 days',
    appt.routine_attended_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended THEN appt.appointment_id END) COMMENT = 'All routine attended',

    -- Duration and wait
    appt.avg_duration AS AVG(appt.duration_minutes) COMMENT = 'Average duration (minutes)',
    appt.total_duration AS SUM(appt.duration_minutes) COMMENT = 'Total appointment minutes',
    appt.avg_days_to_appointment AS AVG(appt.days_to_appointment) COMMENT = 'Average days from booking to appointment',
    appt.avg_patient_wait AS AVG(appt.patient_wait) COMMENT = 'Average wait beyond scheduled time (minutes)'
)

COMMENT = 'OLIDS GP Appointments with patient demographics and conditions. Grain: one row per appointment. Supports GP contract access KPIs, DNA equity analysis, workforce mix, and utilisation by condition/deprivation.'
AI_SQL_GENERATION 'For GP contract KPIs: urgent_same_day_count / urgent_attended_count = same-day rate; routine_within_7d_count / routine_attended_count = 7-day rate. For equity analysis, group by imd_quintile_25 or ethnicity_category. For condition-specific utilisation, filter on has_diabetes etc. Group by DATE_TRUNC(month, start_date) for trends. Cost estimation: AGG(total_duration) * cost_per_minute_gbp grouped by practitioner_role_group.'
AI_QUESTION_CATEGORIZATION 'Use this view for: GP appointment access, same-day urgent access, wait times, DNA rates by deprivation/ethnicity, contact mode trends, workforce mix, utilisation by condition, and GP contract KPIs. For current population snapshots without appointment data use sem_olids_population. For clinical biomarkers use sem_olids_observations. For time-series condition trends use sem_olids_trends.'
