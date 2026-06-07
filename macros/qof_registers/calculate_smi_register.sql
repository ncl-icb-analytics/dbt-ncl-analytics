{% macro calculate_smi_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates SMI (Severe Mental Illness) register status at a given reference date.

    Per QOF MH001:
    - MH1_REG: Ever diagnosed with MH_COD (remission codes do not qualify on their own)
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
          AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND is_diagnosis_code = TRUE
    ),

    lithium_medications_recent AS (
        SELECT
            person_id,
            MAX(CAST(order_date AS DATE)) AS lit_dat,
            COUNT(*) AS recent_lithium_orders
        FROM {{ ref('int_lithium_medications_all') }}
        WHERE CAST(order_date AS DATE) <= {{ reference_date_expr }}
          AND CAST(order_date AS DATE) > DATEADD('month', -6, {{ reference_date_expr }})
          AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
        GROUP BY person_id
    ),

    lithium_stops_filtered AS (
        SELECT
            person_id,
            MAX(CAST(clinical_effective_date AS DATE)) AS litstp_dat
        FROM ({{ get_observations("'LITSTP_COD'", source='PCD') }})
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
        GROUP BY person_id
    ),

    lithium_medications_filtered AS (
        SELECT
            lith.person_id,
            lith.recent_lithium_orders
        FROM lithium_medications_recent lith
        LEFT JOIN lithium_stops_filtered stops
            ON lith.person_id = stops.person_id
            AND stops.litstp_dat > lith.lit_dat
        WHERE stops.person_id IS NULL
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
