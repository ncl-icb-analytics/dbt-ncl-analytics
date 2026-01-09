{% macro calculate_rheumatoid_arthritis_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Rheumatoid Arthritis register status at a given reference date.

    Business Logic:
    - Age ≥16 at first RA diagnosis
    - Active RA diagnosis (no resolution codes in RA)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH ra_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    ra_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date
        FROM ra_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_diagnosis AS (
        SELECT
            diag.person_id,
            diag.earliest_diagnosis_date,
            bd.birth_date_approx,
            DATEDIFF('year', bd.birth_date_approx, diag.earliest_diagnosis_date) AS age_at_first_diagnosis
        FROM ra_person_aggregates diag
        INNER JOIN {{ ref('dim_person_birth_death') }} bd ON diag.person_id = bd.person_id
        WHERE bd.birth_date_approx IS NOT NULL
    ),

    ra_register_logic AS (
        SELECT
            age.person_id,
            'Rheumatoid Arthritis' AS register_name,
            COALESCE(
                age.age_at_first_diagnosis >= 16,
                FALSE
            ) AS is_on_register
        FROM age_at_diagnosis age
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM ra_register_logic

{% endmacro %}
