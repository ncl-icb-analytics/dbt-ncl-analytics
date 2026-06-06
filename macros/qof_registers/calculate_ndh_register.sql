{% macro calculate_ndh_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates NDH (Non-Diabetic Hyperglycaemia) register status at a given reference date.

    Business Logic (QOF v50 NDH register):
    - PAT_AGE >= 18 at the achievement/reference date (NOT age at diagnosis)
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

    age_at_reference AS (
        SELECT
            person_id,
            DATEDIFF('year', birth_date_approx, {{ reference_date_expr }}) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    ndh_register_logic AS (
        SELECT
            ndh.person_id,
            'NDH' AS register_name,
            -- PAT_AGE >= 18 at reference date AND an NDH diagnosis AND no active diabetes
            COALESCE(
                age.age >= 18
                AND ndh.earliest_diagnosis_date IS NOT NULL
                AND (
                    dm.latest_diabetes_date IS NULL
                    OR dm.latest_resolved_date > dm.latest_diabetes_date
                ),
                FALSE
            ) AS is_on_register
        FROM ndh_person_aggregates ndh
        LEFT JOIN age_at_reference age ON ndh.person_id = age.person_id
        LEFT JOIN diabetes_person_aggregates dm ON ndh.person_id = dm.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM ndh_register_logic

{% endmacro %}
