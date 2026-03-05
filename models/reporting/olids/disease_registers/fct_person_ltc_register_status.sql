{{
    config(
        materialized='table',
        cluster_by=['person_id', 'condition_code'])
}}

-- Person-level LTC register status across all condition codes.
-- Adds denominator eligibility flags without changing existing numerator-only register models.

WITH register_rules AS (
    SELECT
        UPPER(condition_code) AS condition_code,
        condition_name,
        clinical_domain,
        CAST(is_qof AS BOOLEAN) AS is_qof,
        CAST(min_age AS INTEGER) AS min_age,
        CAST(max_age AS INTEGER) AS max_age
    FROM {{ ref('ltc_register_denominator_rules') }}
),

person_population AS (
    SELECT
        d.person_id,
        d.is_active,
        d.is_deceased,
        age.age
    FROM {{ ref('dim_person_demographics') }} AS d
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON d.person_id = age.person_id
),

ltc_numerator AS (
    SELECT
        person_id,
        UPPER(condition_code) AS condition_code,
        TRUE AS is_on_register
    FROM {{ ref('fct_person_ltc_summary') }}
),

person_condition_status AS (
    SELECT
        p.person_id,
        r.condition_code,
        r.condition_name,
        r.clinical_domain,
        r.is_qof,
        p.age,
        p.is_active,
        p.is_deceased,
        r.min_age,
        r.max_age,
        COALESCE(n.is_on_register, FALSE) AS is_on_register,
        COALESCE(
            p.age >= r.min_age
            AND (r.max_age IS NULL OR p.age <= r.max_age),
            FALSE
        ) AS is_eligible_denominator,
        CURRENT_DATE() AS reference_date
    FROM person_population AS p
    CROSS JOIN register_rules AS r
    LEFT JOIN ltc_numerator AS n
        ON p.person_id = n.person_id
        AND r.condition_code = n.condition_code
)

SELECT
    person_id,
    condition_code,
    condition_name,
    clinical_domain,
    is_qof,
    age,
    is_active,
    is_deceased,
    min_age,
    max_age,
    is_eligible_denominator,
    is_on_register,
    reference_date
FROM person_condition_status
