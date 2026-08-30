{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['efi2', 'monthly-full']
    )
}}

-- Active, living people eligible for eFI2 at each historical month-end.

SELECT
    person_id,
    month_end_date AS end_date,
    CASE
        WHEN UPPER(gender) IN ('FEMALE', 'F') THEN 'FEMALE'
        WHEN UPPER(gender) IN ('MALE', 'M') THEN 'MALE'
        ELSE 'OTHER/UNKNOWN'
    END AS gender,
    birth_date_approx AS date_of_birth,
    death_date_approx AS date_of_death
FROM {{ ref('int_segmentation_person_month_spine') }}
WHERE is_active
    AND DATEADD('year', 65, DATE_TRUNC('month', birth_date_approx))
        <= month_end_date
