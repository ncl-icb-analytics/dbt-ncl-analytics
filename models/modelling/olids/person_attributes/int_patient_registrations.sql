{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Patient Registrations - Uses episode_of_care with registration type filtering
-- Processes episode_of_care filtered to registration episodes only (concept code 24531000000104)
-- Uses PATIENT_PERSON bridge table to get canonical person_id
-- Handles deceased patients and active registration determination
-- Aligns with PDS comparison methodology

WITH patient_to_person AS (
    SELECT
        pp.patient_id,
        pp.person_id
    FROM {{ ref('stg_olids_patient_person') }} AS pp
    WHERE pp.patient_id IS NOT NULL
        AND pp.person_id IS NOT NULL
),

patient_deceased_status AS (
    SELECT
        p.id AS patient_id,
        p.death_year,
        p.death_month,
        p.death_year IS NOT NULL AS is_deceased,
        CASE
            WHEN p.death_year IS NOT NULL AND p.death_month IS NOT NULL
                THEN DATEADD(
                    DAY,
                    FLOOR(
                        DAY(LAST_DAY(TO_DATE(p.death_year || '-' || p.death_month || '-01'))) / 2
                    ),
                    TO_DATE(p.death_year || '-' || p.death_month || '-01')
                )
        END AS death_date_approx
    FROM {{ ref('stg_olids_patient') }} AS p
),

episode_type_registration AS (
    -- Get concept_id for 'Registration type' (code 24531000000104)
    SELECT DISTINCT c.id AS concept_id
    FROM {{ ref('stg_olids_concept') }} c
    WHERE c.code = '24531000000104'
        AND c.display = 'Registration type'
),

raw_registrations AS (
    -- Get registration episodes from EPISODE_OF_CARE filtered by registration type
    SELECT
        eoc.id AS registration_record_id,
        eoc.patient_id,
        ptp.person_id,
        eoc.organisation_id,
        eoc.episode_of_care_start_date AS registration_start_datetime,
        eoc.episode_of_care_end_date AS registration_end_datetime,
        eoc.care_manager_practitioner_id AS practitioner_id,
        eoc.id AS episode_of_care_id,
        -- Get practice details from record_owner_organisation_code for consistency
        eoc.record_owner_organisation_code AS practice_ods_code,
        dp.practice_name,
        -- Get patient details
        p.sk_patient_id,
        pds.is_deceased,
        pds.death_date_approx
    FROM {{ ref('stg_olids_episode_of_care') }} AS eoc
    INNER JOIN patient_to_person AS ptp
        ON eoc.patient_id = ptp.patient_id
    -- Filter to registration type episodes only via concept map
    LEFT JOIN {{ ref('stg_olids_concept_map') }} cm
        ON eoc.episode_type_source_concept_id = cm.source_code_id
    INNER JOIN episode_type_registration etr
        ON cm.target_code_id = etr.concept_id
    LEFT JOIN {{ ref('dim_practice') }} AS dp
        ON eoc.record_owner_organisation_code = dp.practice_code
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON eoc.patient_id = p.id
    LEFT JOIN patient_deceased_status AS pds
        ON eoc.patient_id = pds.patient_id
    WHERE eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.patient_id IS NOT NULL
        AND eoc.organisation_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            ptp.person_id,
            eoc.organisation_id,
            eoc.episode_of_care_start_date,
            eoc.episode_of_care_end_date,
            eoc.care_manager_practitioner_id
        ORDER BY eoc.id
    ) = 1
),

cleaned_registrations AS (
    -- Clean and validate registration periods with deceased patient logic
    SELECT
        rr.*,
        -- Registration is current if end date suggests active AND patient not deceased
        (
            (
                rr.registration_end_datetime IS NULL
                OR rr.registration_end_datetime > CURRENT_DATE()
                OR rr.registration_end_datetime < rr.registration_start_datetime
            )
            AND (
                NOT rr.is_deceased
                OR rr.death_date_approx IS NULL
                OR rr.death_date_approx > CURRENT_DATE()
            )
        ) AS is_current_registration,

        -- Calculate registration duration (only for completed registrations with valid end dates)
        CASE
            WHEN rr.registration_end_datetime IS NOT NULL
                AND rr.registration_end_datetime >= rr.registration_start_datetime
                THEN DATEDIFF('day', rr.registration_start_datetime, rr.registration_end_datetime)
        END AS registration_duration_days,

        -- Effective end date considers both registration end and death
        CASE
            -- If deceased, use death date as effective end if earlier
            WHEN rr.is_deceased
                AND rr.death_date_approx IS NOT NULL
                AND rr.death_date_approx >= rr.registration_start_datetime
                AND rr.death_date_approx <= CURRENT_DATE()
                AND (
                    rr.registration_end_datetime IS NULL
                    OR rr.death_date_approx < rr.registration_end_datetime
                )
                THEN rr.death_date_approx
            -- If registration ended validly, use registration end date
            WHEN rr.registration_end_datetime IS NOT NULL
                AND rr.registration_end_datetime >= rr.registration_start_datetime
                AND rr.registration_end_datetime <= CURRENT_DATE()
                THEN rr.registration_end_datetime
            ELSE NULL
        END AS effective_end_date,

        -- Registration status classification
        CASE
            WHEN rr.is_deceased
                AND rr.death_date_approx IS NOT NULL
                AND rr.death_date_approx <= CURRENT_DATE()
                THEN 'Historical - Deceased'
            WHEN (
                rr.registration_end_datetime IS NULL
                OR rr.registration_end_datetime > CURRENT_DATE()
                OR rr.registration_end_datetime < rr.registration_start_datetime
            ) THEN 'Active'
            ELSE 'Historical'
        END AS registration_status

    FROM raw_registrations AS rr
),

person_registration_sequences AS (
    -- Add sequence information for each person's registrations
    SELECT
        cr.*,
        -- Number registrations chronologically per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_datetime, cr.registration_record_id
        ) AS registration_sequence,

        -- Identify latest registration per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY
                cr.registration_start_datetime DESC, cr.registration_record_id DESC
        ) = 1 AS is_latest_registration,

        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY cr.person_id) AS total_registrations_count,

        -- Get next registration start date (for gap analysis)
        LEAD(cr.registration_start_datetime) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_datetime, cr.registration_record_id
        ) AS next_registration_start,

        -- Get previous registration end date
        LAG(cr.effective_end_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_datetime, cr.registration_record_id
        ) AS previous_registration_end

    FROM cleaned_registrations AS cr
)

-- Final selection with complete registration analysis
SELECT
    -- Core identifiers
    prs.registration_record_id,
    prs.person_id,
    prs.patient_id,
    prs.sk_patient_id,
    prs.organisation_id,
    prs.practice_name,
    prs.practice_ods_code,

    -- Registration period details (expose as dates for downstream consumers)
    prs.registration_start_datetime::date AS registration_start_date,
    prs.registration_end_datetime::date AS registration_end_date,
    prs.effective_end_date,
    prs.registration_duration_days,
    prs.registration_status,

    -- Registration flags
    prs.is_current_registration,
    prs.is_latest_registration,

    -- Sequence information
    prs.registration_sequence,
    prs.total_registrations_count,
    prs.registration_sequence > 1 AS has_changed_practice,

    -- Gap analysis
    CASE
        WHEN
            prs.previous_registration_end IS NOT NULL
            AND prs.registration_start_datetime
            > DATEADD('day', 1, prs.previous_registration_end)
            THEN
                DATEDIFF(
                    'day',
                    prs.previous_registration_end,
                    prs.registration_start_datetime
                )
    END AS gap_since_previous_registration_days,

    -- Registration metadata
    prs.practitioner_id,
    prs.episode_of_care_id

FROM person_registration_sequences AS prs
ORDER BY prs.person_id, prs.registration_start_datetime
