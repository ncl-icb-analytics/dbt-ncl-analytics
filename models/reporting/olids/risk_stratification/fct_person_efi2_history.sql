{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['efi2', 'monthly-full']
    )
}}

WITH scored AS (
    SELECT
        pl.person_id,
        pl.end_date,
        COALESCE(score.efi, 0) AS efi_score,
        FLOOR(DATEDIFF('month', pl.date_of_birth, pl.end_date) / 12)
            AS age_at_end,
        pl.gender,
        pl.date_of_death
    FROM {{ ref('int_efi2_patient_list_history') }} AS pl
    LEFT JOIN {{ ref('int_efi2_chronology_history') }} AS score
        ON pl.person_id = score.person_id
        AND pl.end_date = score.end_date
)

SELECT
    person_id,
    end_date,
    efi_score,
    CASE
        WHEN efi_score < 0.0857 THEN 'ROBUST'
        WHEN efi_score < 0.1624 THEN 'MILD FRAILTY'
        WHEN efi_score <= 0.2391 THEN 'MODERATE FRAILTY'
        ELSE 'SEVERE FRAILTY'
    END AS category,
    age_at_end,
    gender,
    date_of_death
FROM scored
