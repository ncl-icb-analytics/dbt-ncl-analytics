{% macro calculate_hypertension_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Hypertension register status at a given reference date.

    Business Logic:
    - Age ≥18 at reference date
    - Active hypertension diagnosis (latest diagnosis > latest resolution)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH hypertension_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_hypertension_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    hypertension_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM hypertension_diagnoses_filtered
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

    hypertension_register_logic AS (
        SELECT
            diag.person_id,
            'Hypertension' AS register_name,
            COALESCE(
                age.age >= 18
                AND diag.latest_diagnosis_date > COALESCE(diag.latest_resolved_date, '1900-01-01'),
                FALSE
            ) AS is_on_register
        FROM hypertension_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM hypertension_register_logic

{% endmacro %}
