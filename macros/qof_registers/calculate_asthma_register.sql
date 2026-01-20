{% macro calculate_asthma_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates Asthma register status at a given reference date.

    Business Logic:
    - Age ≥6 at reference date
    - Active asthma diagnosis (latest diagnosis > latest resolution)
    - Recent asthma medication (within 12 months prior to reference date)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH asthma_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_asthma_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    asthma_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
            COALESCE(
                MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
                AND (
                    MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NULL
                    OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
                       > MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
                ),
                FALSE
            ) AS has_active_asthma_diagnosis
        FROM asthma_diagnoses_filtered
        GROUP BY person_id
    ),

    asthma_medications_filtered AS (
        SELECT
            mo.person_id,
            MAX(mo.order_date) AS latest_medication_date
        FROM ({{ get_medication_orders(cluster_id='ASTTRT_COD') }}) mo
        WHERE mo.order_date >= {{ reference_date_expr }} - INTERVAL '12 months'
          AND mo.order_date <= {{ reference_date_expr }}
        GROUP BY mo.person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            DATEDIFF('year', birth_date_approx, {{ reference_date_expr }}) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    asthma_register_logic AS (
        SELECT
            diag.person_id,
            'Asthma' AS register_name,
            COALESCE(
                age.age >= 6
                AND diag.has_active_asthma_diagnosis = TRUE
                AND med.latest_medication_date IS NOT NULL,
                FALSE
            ) AS is_on_register
        FROM asthma_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
        LEFT JOIN asthma_medications_filtered med ON diag.person_id = med.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM asthma_register_logic

{% endmacro %}
