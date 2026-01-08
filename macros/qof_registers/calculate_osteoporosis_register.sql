{% macro calculate_osteoporosis_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Osteoporosis register status at a given reference date.

    Business Logic:
    - Age 50-74 years at reference date
    - Fragility fracture after April 2012
    - Osteoporosis diagnosis (OSTEO_COD)
    - DXA confirmation (scan OR T-score ≤ -2.5)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH osteoporosis_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_osteoporosis_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND is_diagnosis_code = TRUE
    ),

    osteoporosis_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM osteoporosis_diagnoses_filtered
        GROUP BY person_id
    ),

    fragility_fractures_filtered AS (
        SELECT
            person_id,
            clinical_effective_date
        FROM {{ ref('int_fragility_fractures_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND clinical_effective_date >= '2012-04-01'
    ),

    fragility_person_aggregates AS (
        SELECT
            person_id,
            COUNT(*) > 0 AS has_fragility_fracture
        FROM fragility_fractures_filtered
        GROUP BY person_id
    ),

    dxa_scans_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_dxa_scan_procedure,
            is_dxa_t_score_measurement,
            validated_t_score
        FROM {{ ref('int_dxa_scans_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    dxa_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_dxa_scan_procedure = TRUE THEN 1 ELSE 0 END) = 1 AS has_dxa_scan,
            MAX(CASE WHEN is_dxa_t_score_measurement = TRUE AND validated_t_score <= -2.5 THEN 1 ELSE 0 END) = 1 AS has_valid_t_score
        FROM dxa_scans_filtered
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

    osteoporosis_register_logic AS (
        SELECT
            diag.person_id,
            'Osteoporosis' AS register_name,
            COALESCE(
                age.age BETWEEN 50 AND 74
                AND frac.has_fragility_fracture = TRUE
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (dxa.has_dxa_scan = TRUE OR dxa.has_valid_t_score = TRUE),
                FALSE
            ) AS is_on_register
        FROM osteoporosis_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
        LEFT JOIN fragility_person_aggregates frac ON diag.person_id = frac.person_id
        LEFT JOIN dxa_person_aggregates dxa ON diag.person_id = dxa.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM osteoporosis_register_logic

{% endmacro %}
