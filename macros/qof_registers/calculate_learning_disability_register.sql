{% macro calculate_learning_disability_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Learning Disability register status at a given reference date.

    Business Logic:
    - Presence of learning disability diagnosis
    - Age ≥14 at reference date
    - Lifelong condition, no resolution

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH learning_disability_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_learning_disability_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND is_diagnosis_code = TRUE
    ),

    learning_disability_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM learning_disability_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            DATEDIFF('year', birth_date_approx, {{ reference_date_expr }}) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    learning_disability_register_logic AS (
        SELECT
            diag.person_id,
            'Learning Disability' AS register_name,
            COALESCE(
                diag.earliest_diagnosis_date IS NOT NULL
                AND age.age >= 14,
                FALSE
            ) AS is_on_register
        FROM learning_disability_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM learning_disability_register_logic

{% endmacro %}
