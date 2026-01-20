{% macro calculate_smi_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates SMI (Severe Mental Illness) register status at a given reference date.

    Business Logic:
    - Active SMI diagnosis (latest diagnosis > latest resolution OR no resolution)
    - OR lithium therapy in last 6 months from reference date
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH smi_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_smi_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    smi_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM smi_diagnoses_filtered
        GROUP BY person_id
    ),

    lithium_medications_filtered AS (
        SELECT
            person_id,
            order_date
        FROM {{ ref('int_lithium_medications_all') }}
        WHERE order_date <= {{ reference_date_expr }}
          AND order_date >= DATEADD('month', -6, {{ reference_date_expr }})
    ),

    lithium_person_aggregates AS (
        SELECT
            person_id,
            COUNT(*) AS recent_lithium_orders
        FROM lithium_medications_filtered
        GROUP BY person_id
    ),

    smi_register_logic AS (
        SELECT
            diag.person_id,
            'SMI' AS register_name,
            COALESCE(
                (diag.latest_diagnosis_date IS NOT NULL
                 AND (diag.latest_resolved_date IS NULL OR diag.latest_diagnosis_date > diag.latest_resolved_date))
                OR lith.recent_lithium_orders > 0,
                FALSE
            ) AS is_on_register
        FROM smi_person_aggregates diag
        LEFT JOIN lithium_person_aggregates lith ON diag.person_id = lith.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM smi_register_logic

{% endmacro %}
