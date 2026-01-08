{% macro calculate_depression_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Depression register status at a given reference date.

    Business Logic:
    - Age ≥18 at reference date
    - Active depression diagnosis (latest diagnosis > latest resolution)
    - Latest diagnosis on/after 2006-04-01 (QOF date threshold)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH depression_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_depression_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    depression_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM depression_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            DATEDIFF('year', birth_date_approx, {{ reference_date_expr }}) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
          AND (death_date_approx IS NULL OR death_date_approx > {{ reference_date_expr }})
    ),

    depression_register_logic AS (
        SELECT
            diag.person_id,
            'Depression' AS register_name,
            COALESCE(
                age.age >= 18
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND diag.latest_diagnosis_date >= '2006-04-01'
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_resolved_date
                ),
                FALSE
            ) AS is_on_register
        FROM depression_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM depression_register_logic

{% endmacro %}
