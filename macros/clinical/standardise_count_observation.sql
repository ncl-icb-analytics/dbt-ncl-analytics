{% macro standardise_count_observation(base_cte, measurement, value_column='result_value', max_plausible_value=100, enable_magnitude_conversion=true) %}

{#
    Standardises count-type observation values using a value-first approach.

    Generates CTEs (unit_rules, classified, standardised) that take a base CTE of
    raw observations, join to the observation_unit_rules seed, and produce standardised
    output with inferred values, units, confidence levels, and conversion metadata.

    Uses a 3-pass approach:
      Pass 1 - Accept values already in plausible range (0 to max_plausible_value),
               regardless of recorded unit. Most records are handled here.
      Pass 2 - For out-of-range values, apply known conversion factors from the seed
               (e.g. {cells}/uL -> 10*9/L via x0.001).
      Pass 3 - For remaining out-of-range values, infer the unit from the magnitude
               of the value using SI prefix conversions (/1000, /1M, /1B).
               Only runs if enable_magnitude_conversion is true.

    Unit classification after LEFT JOIN to seed:
      known    - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NOT NULL
      excluded - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NULL
      unknown  - CONVERT_FROM_UNIT IS NULL (no match in seed)

    Confidence levels:
      VERY_HIGH - Canonical unit (e.g. 10*9/L -> 10*9/L), value in range
      HIGH      - Known conversion applied, or known equivalent unit with value in range
      MEDIUM    - Unknown/nonsense unit but value in plausible range
      LOW       - Value inferred from magnitude (Pass 3)
      NONE      - Excluded unit, or value out of range after all passes

    Parameters:
      base_cte (str):
          Name of the CTE containing raw observations. Must include result_unit_code,
          result_unit_display, and the value_column.

      measurement (str):
          Identifier matching DEFINITION_NAME in the observation_unit_rules seed
          (e.g. 'eosinophil_count').

      value_column (str):
          Column name containing the numeric result. Default: 'result_value'.

      max_plausible_value (number):
          Upper bound for plausible range in standard units. Default: 100.

      enable_magnitude_conversion (bool):
          Whether to attempt Pass 3 magnitude-based inference. Default: true.

    Output columns (appended to all columns from base_cte):
      original_result_value       - Raw value preserved from source
      original_result_unit_code   - Raw unit code preserved from source
      original_result_unit_display - Raw unit display preserved from source
      inferred_value              - Standardised numeric value (NULL if NONE confidence)
      inferred_unit               - Standardised unit (NULL if NONE confidence)
      confidence                  - VERY_HIGH / HIGH / MEDIUM / LOW / NONE
      value_was_converted         - TRUE if value was mathematically changed
      conversion_reason           - Human-readable explanation of action taken
#}

-- Filter seed to rules for this measurement only
unit_rules AS (
    SELECT *
    FROM {{ ref('observation_unit_rules') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
),

-- Join observations to seed and classify each record's unit status
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
        -- Unit classification: known (matched with conversion), excluded (matched without), unknown (no match)
        CASE
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NULL THEN 'excluded'
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NOT NULL THEN 'known'
            ELSE 'unknown'
        END AS unit_status,
        -- Canonical = unit converts to itself with factor 1 (e.g. 10*9/L -> 10*9/L)
        (ur.CONVERT_FROM_UNIT = ur.CONVERT_TO_UNIT AND ur.MULTIPLY_BY = 1) AS is_canonical
    FROM {{ base_cte }} base
    LEFT JOIN unit_rules ur
        ON base.result_unit_code = ur.CONVERT_FROM_UNIT
),

-- Apply the 3-pass standardisation logic
standardised AS (
    SELECT
        *,

        -- inferred_value: the standardised numeric result
        CASE
            -- Excluded units are never valid for this measurement
            WHEN unit_status = 'excluded'
                THEN NULL

            -- Pass 1: value already in plausible range -> accept as-is
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }}
             AND unit_status != 'excluded'
                THEN numeric_value

            -- Pass 2: out of range + known conversion factor -> apply formula
            WHEN unit_status = 'known'
             AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN (numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)

            {% if enable_magnitude_conversion %}
            -- Pass 3: infer unit from value magnitude (SI prefix conversions)
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

            -- No pass succeeded
            ELSE NULL
        END AS inferred_value,

        -- confidence: how confident we are in the inferred value
        CASE
            WHEN unit_status = 'excluded' THEN 'NONE'
            -- Pass 1 confidence depends on unit classification
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded' THEN
                CASE
                    WHEN is_canonical THEN 'VERY_HIGH'
                    WHEN unit_status = 'known' THEN 'HIGH'
                    ELSE 'MEDIUM'
                END
            -- Pass 2
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'HIGH'
            {% if enable_magnitude_conversion %}
            -- Pass 3
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

        -- inferred_unit: the standardised unit code
        CASE
            WHEN unit_status = 'excluded'
                THEN NULL
            -- Pass 1: use the seed's target unit (or NULL for unknown units that still fall in range)
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded'
                THEN seed_convert_to_unit
            -- Pass 2
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN seed_convert_to_unit
            {% if enable_magnitude_conversion %}
            -- Pass 3: magnitude conversion always targets 10*9/L
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

        -- value_was_converted: TRUE if the numeric value was mathematically changed
        CASE
            WHEN unit_status = 'excluded'
                THEN FALSE
            -- Pass 1: value accepted as-is
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded'
                THEN FALSE
            -- Pass 2: conversion factor applied
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN TRUE
            {% if enable_magnitude_conversion %}
            -- Pass 3: magnitude division applied
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

        -- conversion_reason: human-readable explanation of action taken
        CASE
            WHEN unit_status = 'excluded'
                THEN 'Excluded unit on this measurement type'
            -- Pass 1 reasons
            WHEN numeric_value BETWEEN 0 AND {{ max_plausible_value }} AND unit_status != 'excluded' THEN
                CASE
                    WHEN is_canonical THEN 'Canonical unit, value in range'
                    WHEN unit_status = 'known' THEN 'Known equivalent unit, value in range'
                    ELSE 'Unknown unit, value in plausible range - accepted as standard unit'
                END
            -- Pass 2 reason
            WHEN unit_status = 'known' AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by + COALESCE(seed_post_offset, 0)) BETWEEN 0 AND {{ max_plausible_value }}
                THEN 'Value converted using known unit factor (x' || seed_multiply_by::VARCHAR || ')'
            {% if enable_magnitude_conversion %}
            -- Pass 3 reasons
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
