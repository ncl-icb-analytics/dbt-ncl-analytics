{% macro calculate_ndh_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates NDH (Non-Diabetic Hyperglycaemia) register status at a given reference date.

    Business Logic:
    - Age ≥18 at first NDH diagnosis
    - Has NDH/IGT/PRD diagnosis
    - Never had diabetes OR diabetes is resolved

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH ndh_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_any_ndh_type_code
        FROM {{ ref('int_ndh_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND is_any_ndh_type_code = TRUE
    ),

    ndh_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM ndh_diagnoses_filtered
        GROUP BY person_id
    ),

    diabetes_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_general_diabetes_code,
            is_diabetes_resolved_code
        FROM {{ ref('int_diabetes_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    diabetes_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS latest_diabetes_date,
            MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM diabetes_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_diagnosis AS (
        SELECT
            ndh.person_id,
            ndh.earliest_diagnosis_date,
            bd.birth_date_approx,
            DATEDIFF('year', bd.birth_date_approx, ndh.earliest_diagnosis_date) AS age_at_first_ndh
        FROM ndh_person_aggregates ndh
        INNER JOIN {{ ref('dim_person_birth_death') }} bd ON ndh.person_id = bd.person_id
        WHERE bd.birth_date_approx IS NOT NULL
    ),

    ndh_register_logic AS (
        SELECT
            age.person_id,
            'NDH' AS register_name,
            COALESCE(
                age.age_at_first_ndh >= 18
                AND (
                    dm.latest_diabetes_date IS NULL
                    OR dm.latest_resolved_date > dm.latest_diabetes_date
                ),
                FALSE
            ) AS is_on_register
        FROM age_at_diagnosis age
        LEFT JOIN diabetes_person_aggregates dm ON age.person_id = dm.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM ndh_register_logic

{% endmacro %}
