{% macro standardise_count_observation(base_cte, measurement, value_column='result_value', max_plausible_value=100, enable_magnitude_conversion=true) %}

unit_rules AS (
    SELECT *
    FROM {{ ref('observation_unit_rules') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
),

classified AS (
    SELECT
        base.*,
        TRY_CAST(base.{{ value_column }} AS FLOAT) AS numeric_value,
        base.result_unit_code AS original_result_unit_code,
        base.result_unit_display AS original_result_unit_display,
        base.{{ value_column }} AS original_result_value,
        ur.CONVERT_TO_UNIT AS seed_convert_to_unit,
        ur.MULTIPLY_BY AS seed_multiply_by,
        ur.PRE_OFFSET AS seed_pre_offset,
        ur.POST_OFFSET AS seed_post_offset,
        CASE
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NULL THEN 'excluded'
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NOT NULL THEN 'known'
            ELSE 'unknown'
        END AS unit_status,
        (ur.CONVERT_FROM_UNIT = ur.CONVERT_TO_UNIT AND ur.MULTIPLY_BY = 1) AS is_canonical
    FROM {{ base_cte }} base
    LEFT JOIN unit_rules ur
        ON base.result_unit_code = ur.CONVERT_FROM_UNIT
),

standardised AS (
    SELECT
        *,

        CASE
            WHEN unit_status = 'excluded'
                THEN NULL

            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }}
             AND unit_status != 'excluded'
                THEN numeric_value

            WHEN unit_status = 'known'
             AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN (numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)

            {% if enable_magnitude_conversion %}
            WHEN numeric_value > {{ max_plausible_value }} AND numeric_value <= 1000
             AND (numeric_value / 1000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN numeric_value / 1000

            WHEN numeric_value > 1000 AND numeric_value <= 1000000
             AND (numeric_value / 1000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN numeric_value / 1000000

            WHEN numeric_value > 1000000
             AND (numeric_value / 1000000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN numeric_value / 1000000000
            {% endif %}

            ELSE NULL
        END AS inferred_value,

        CASE
            WHEN unit_status = 'excluded' THEN 'NONE'
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded' THEN
                CASE
                    WHEN is_canonical THEN 'VERY_HIGH'
                    WHEN unit_status = 'known' THEN 'HIGH'
                    ELSE 'MEDIUM'
                END
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'HIGH'
            {% if enable_magnitude_conversion %}
            WHEN numeric_value > {{ max_plausible_value }} AND numeric_value <= 1000
             AND (numeric_value / 1000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'LOW'
            WHEN numeric_value > 1000 AND numeric_value <= 1000000
             AND (numeric_value / 1000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'LOW'
            WHEN numeric_value > 1000000
             AND (numeric_value / 1000000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'LOW'
            {% endif %}
            ELSE 'NONE'
        END AS confidence,

        CASE
            WHEN unit_status = 'excluded'
                THEN NULL
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded'
                THEN seed_convert_to_unit
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN seed_convert_to_unit
            {% if enable_magnitude_conversion %}
            WHEN numeric_value > {{ max_plausible_value }} AND numeric_value <= 1000000000
             AND (numeric_value / CASE
                    WHEN numeric_value <= 1000 THEN 1000
                    WHEN numeric_value <= 1000000 THEN 1000000
                    ELSE 1000000000
                 END) BETWEEN 0 AND {{ max_plausible_value }}
                THEN '10*9/L'
            {% endif %}
            ELSE NULL
        END AS inferred_unit,

        CASE
            WHEN unit_status = 'excluded'
                THEN FALSE
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded'
                THEN FALSE
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN TRUE
            {% if enable_magnitude_conversion %}
            WHEN numeric_value > {{ max_plausible_value }} AND numeric_value <= 1000
             AND (numeric_value / 1000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN TRUE
            WHEN numeric_value > 1000 AND numeric_value <= 1000000
             AND (numeric_value / 1000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN TRUE
            WHEN numeric_value > 1000000
             AND (numeric_value / 1000000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN TRUE
            {% endif %}
            ELSE FALSE
        END AS value_was_converted,

        CASE
            WHEN unit_status = 'excluded'
                THEN 'Excluded unit on this measurement type'
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded' THEN
                CASE
                    WHEN is_canonical THEN 'Canonical unit, value in range'
                    WHEN unit_status = 'known' THEN 'Known equivalent unit, value in range'
                    ELSE 'Unknown unit, value in plausible range - accepted as standard unit'
                END
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'Value converted using known unit factor (x' || seed_multiply_by::VARCHAR || ')'
            {% if enable_magnitude_conversion %}
            WHEN numeric_value > {{ max_plausible_value }} AND numeric_value <= 1000
             AND (numeric_value / 1000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'Value magnitude suggests cells/uL scale, divided by 1000'
            WHEN numeric_value > 1000 AND numeric_value <= 1000000
             AND (numeric_value / 1000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'Value magnitude suggests cells/mL scale, divided by 1000000'
            WHEN numeric_value > 1000000
             AND (numeric_value / 1000000000) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'Value magnitude suggests cells/L scale, divided by 1000000000'
            {% endif %}
            ELSE 'Value out of range after all conversion attempts'
        END AS conversion_reason

    FROM classified
)

{% endmacro %}
