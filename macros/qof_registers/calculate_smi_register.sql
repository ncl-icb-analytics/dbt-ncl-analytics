{% macro calculate_smi_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates SMI (Severe Mental Illness) register status at a given reference date.

    Per QOF MH001:
    - MH1_REG: Ever diagnosed with MH_COD (no remission exclusion)
    - MH2_REG: Lithium therapy in last 6 months, not subsequently stopped
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH smi_diagnoses_filtered AS (
        SELECT DISTINCT person_id
        FROM {{ ref('int_smi_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    lithium_medications_filtered AS (
        SELECT
            person_id,
            COUNT(*) AS recent_lithium_orders
        FROM {{ ref('int_lithium_medications_all') }}
        WHERE order_date <= {{ reference_date_expr }}
          AND order_date >= DATEADD('month', -6, {{ reference_date_expr }})
        GROUP BY person_id
    ),

    smi_register_logic AS (
        SELECT
            COALESCE(diag.person_id, lith.person_id) AS person_id,
            'SMI' AS register_name,
            COALESCE(
                diag.person_id IS NOT NULL
                OR lith.recent_lithium_orders > 0,
                FALSE
            ) AS is_on_register
        FROM smi_diagnoses_filtered diag
        FULL OUTER JOIN lithium_medications_filtered lith ON diag.person_id = lith.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM smi_register_logic

{% endmacro %}
