{% test condition_codes_match_seed(model, seed_model, model_column='condition_code', seed_column='condition_code') %}

-- Compares condition code sets between a model and a seed.
-- Returns mismatches in either direction.

WITH model_codes AS (
    SELECT DISTINCT UPPER({{ model_column }}) AS condition_code
    FROM {{ model }}
),

seed_codes AS (
    SELECT DISTINCT UPPER({{ seed_column }}) AS condition_code
    FROM {{ ref(seed_model) }}
),

model_rowcount AS (
    SELECT COUNT(*) AS model_code_count
    FROM model_codes
),

missing_in_seed AS (
    SELECT
        'missing_in_seed' AS mismatch_type,
        m.condition_code
    FROM model_codes AS m
    LEFT JOIN seed_codes AS s
        ON m.condition_code = s.condition_code
    WHERE s.condition_code IS NULL
),

missing_in_model AS (
    SELECT
        'missing_in_model' AS mismatch_type,
        s.condition_code
    FROM seed_codes AS s
    LEFT JOIN model_codes AS m
        ON s.condition_code = m.condition_code
    WHERE m.condition_code IS NULL
)

SELECT mismatch_type, condition_code
FROM missing_in_seed
WHERE (SELECT model_code_count FROM model_rowcount) > 0

UNION ALL

SELECT mismatch_type, condition_code
FROM missing_in_model
WHERE (SELECT model_code_count FROM model_rowcount) > 0

{% endtest %}
