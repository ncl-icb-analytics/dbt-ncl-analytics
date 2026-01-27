{{
    config(
        materialized='view',
        tags=['intermediate', 'patient', 'deceased'])
}}

-- Patient-level deceased status with approximate death date
-- Single source of truth for death date approximation logic
-- Uses midpoint of death month, or July 1st if only death year is known

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
        WHEN p.death_year IS NOT NULL
            THEN TO_DATE(p.death_year || '-07-01')
    END AS death_date_approx
FROM {{ ref('stg_olids_patient') }} AS p
