{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice', 'episode_of_care'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Patient Registrations - Episode of Care Based with SCD Logic
-- Uses episode_of_care as primary source for registration periods
-- Incorporates deceased status from patient table to mark registrations as inactive
-- Builds slowly changing dimension with proper historical tracking
-- Compatible interface with int_patient_registrations for easy switching

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

raw_episodes AS (
    SELECT
        eoc.id AS registration_record_id,
        eoc.patient_id,
        ptp.person_id,
        eoc.organisation_id,
        o.organisation_code AS practice_ods_code,
        o.name AS practice_name,
        eoc.episode_of_care_start_date AS registration_start_date,
        eoc.episode_of_care_end_date AS registration_end_date,
        eoc.care_manager_practitioner_id AS practitioner_id,
        eoc.id AS episode_of_care_id,
        p.sk_patient_id,
        pds.is_deceased,
        pds.death_date_approx
    FROM {{ ref('stg_olids_episode_of_care') }} AS eoc
    INNER JOIN patient_to_person AS ptp
        ON eoc.patient_id = ptp.patient_id
    LEFT JOIN {{ ref('stg_olids_organisation') }} AS o
        ON eoc.organisation_id = o.id
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
    SELECT
        re.*,
        -- Registration is current if end date suggests active AND patient not deceased
        (
            (
                re.registration_end_date IS NULL
                OR re.registration_end_date > CURRENT_DATE()
                OR re.registration_end_date < re.registration_start_date
            )
            AND (
                NOT re.is_deceased
                OR re.death_date_approx IS NULL
                OR re.death_date_approx > CURRENT_DATE()
            )
        ) AS is_current_registration,

        -- Calculate registration duration
        CASE
            WHEN re.registration_end_date IS NOT NULL
                AND re.registration_end_date >= re.registration_start_date
                THEN DATEDIFF('day', re.registration_start_date, re.registration_end_date)
        END AS registration_duration_days,

        -- Effective end date considers both registration end and death
        CASE
            -- If deceased, use death date as effective end if earlier
            WHEN re.is_deceased
                AND re.death_date_approx IS NOT NULL
                AND re.death_date_approx >= re.registration_start_date
                AND re.death_date_approx <= CURRENT_DATE()
                AND (
                    re.registration_end_date IS NULL
                    OR re.death_date_approx < re.registration_end_date
                )
                THEN re.death_date_approx
            -- If registration ended validly, use registration end date
            WHEN re.registration_end_date IS NOT NULL
                AND re.registration_end_date >= re.registration_start_date
                AND re.registration_end_date <= CURRENT_DATE()
                THEN re.registration_end_date
            ELSE NULL
        END AS effective_end_date,

        -- Registration status classification
        CASE
            WHEN re.is_deceased
                AND re.death_date_approx IS NOT NULL
                AND re.death_date_approx <= CURRENT_DATE()
                THEN 'Historical - Deceased'
            WHEN (
                re.registration_end_date IS NULL
                OR re.registration_end_date > CURRENT_DATE()
                OR re.registration_end_date < re.registration_start_date
            ) THEN 'Active'
            ELSE 'Historical'
        END AS registration_status

    FROM raw_episodes AS re
),

person_registration_sequences AS (
    SELECT
        cr.*,
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS registration_sequence,

        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date DESC, cr.registration_record_id DESC
        ) = 1 AS is_latest_registration,

        COUNT(*) OVER (PARTITION BY cr.person_id) AS total_registrations_count,

        LEAD(cr.registration_start_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS next_registration_start,

        LAG(cr.effective_end_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS previous_registration_end

    FROM cleaned_registrations AS cr
)

SELECT
    prs.registration_record_id,
    prs.person_id,
    prs.patient_id,
    prs.sk_patient_id,
    prs.organisation_id,
    prs.practice_name,
    prs.practice_ods_code,

    prs.registration_start_date,
    prs.registration_end_date,
    prs.effective_end_date,
    prs.registration_duration_days,
    prs.registration_status,

    prs.is_current_registration,
    prs.is_latest_registration,

    prs.registration_sequence,
    prs.total_registrations_count,
    prs.registration_sequence > 1 AS has_changed_practice,

    CASE
        WHEN prs.previous_registration_end IS NOT NULL
            AND prs.registration_start_date > DATEADD('day', 1, prs.previous_registration_end)
            THEN DATEDIFF('day', prs.previous_registration_end, prs.registration_start_date)
    END AS gap_since_previous_registration_days,

    prs.practitioner_id,
    prs.episode_of_care_id

FROM person_registration_sequences AS prs
ORDER BY prs.person_id, prs.registration_start_date