{% macro standardise_count_observation(base_cte, measurement, value_column='result_value') %}

{#
    Standardises count-type observation values and units.

    Produces a standardised output with inferred values and units as well as
    assigning confidence levels to the inference. A given row will have a value
    and corresponding unit.

    Uses a 3-pass approach:
      Pass 1 - Accept values already in plausible range (defined by observation_value_bounds seed),
               regardless of recorded unit.

      Pass 2 - For out-of-range values, look at the unit and apply known conversion factors
               from the seed table if possible (e.g. {cells}/uL -> 10*9/L via x0.001).

      Pass 3 - For remaining out-of-range values, check if the value is unambiguously
               a raw base-unit recording (e.g. cells/L instead of 10*9/L) by dividing
               by 10^CANONICAL_EXPONENT and checking the result falls in
               [EXPECTED_LOWER, UPPER_LIMIT]. Controlled by the observation_value_bounds
               seed — CANONICAL_EXPONENT IS NULL disables Pass 3.

    Unit classification after LEFT JOIN to seed:
      known    - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NOT NULL
      excluded - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NULL
      unknown  - CONVERT_FROM_UNIT IS NULL (no match in seed)

    Confidence levels:
      VERY_HIGH - Canonical unit (e.g. 10*9/L -> 10*9/L), value in range
      HIGH      - Known equivalent unit (MULTIPLY_BY=1) with value in range, or known conversion applied (Pass 2)
      MEDIUM    - Unknown/nonsense unit, or known non-equivalent unit, but value in plausible range
      LOW       - Value inferred from magnitude (Pass 3)
      NONE      - Excluded unit, or value out of range after all passes

    Parameters:
      base_cte (str):
          Name of the CTE containing raw observations. Must include result_unit_code,
          result_unit_display, and the value_column.

      measurement (str):
          Identifier matching DEFINITION_NAME in the observation_standard_units,
          observation_value_bounds, and observation_unit_rules seeds
          (e.g. 'eosinophil_count').

      value_column (str):
          Column name containing the numeric result. Default: 'result_value'.

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

-- Look up the canonical target unit for this measurement (e.g. '10*9/L')
canonical_unit AS (
    SELECT UNIT AS CONVERT_TO_UNIT
    FROM {{ ref('observation_standard_units') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
      AND PRIMARY_UNIT = TRUE
    LIMIT 1
),

-- Look up the plausible value range and Pass 3 configuration for this measurement
value_bounds AS (
    SELECT LOWER_LIMIT, UPPER_LIMIT, CANONICAL_EXPONENT, EXPECTED_LOWER
    FROM {{ ref('observation_value_bounds') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
    LIMIT 1
),

-- Join observations to seed and classify each record's unit status
classified AS (
    SELECT
        base.*,
        TRY_CAST(base.{{ value_column }} AS FLOAT) AS numeric_value,
        base.result_unit_code AS original_result_unit_code,
        base.result_unit_display AS original_result_unit_display,
        base.{{ value_column }} AS original_result_value,
        cu.CONVERT_TO_UNIT AS canonical_unit_code,
        vb.LOWER_LIMIT AS lower_limit,
        vb.UPPER_LIMIT AS upper_limit,
        vb.CANONICAL_EXPONENT AS canonical_exponent,
        vb.EXPECTED_LOWER AS expected_lower,
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
    CROSS JOIN canonical_unit cu
    CROSS JOIN value_bounds vb
),

-- Determine which pass (if any) applies - evaluated ONCE per row
-- All output columns in standardised are then derived from matched_pass
pass_assigned AS (
    SELECT
        *,
        (numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by
            + COALESCE(seed_post_offset, 0) AS converted_value,
        CASE
            WHEN unit_status = 'excluded' THEN 'excluded'
            WHEN numeric_value BETWEEN lower_limit AND upper_limit
                THEN 'pass_1'
            WHEN unit_status = 'known'
             AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by
                  + COALESCE(seed_post_offset, 0))
                  BETWEEN lower_limit AND upper_limit
                THEN 'pass_2'
            -- Pass 3: value is unambiguously in raw base-unit scale
            WHEN canonical_exponent IS NOT NULL
             AND numeric_value > upper_limit
             AND (numeric_value / POWER(10, canonical_exponent)) >= expected_lower
             AND (numeric_value / POWER(10, canonical_exponent)) <= upper_limit
                THEN 'pass_3'
            ELSE 'no_match'
        END AS matched_pass
    FROM classified
),

-- Derive ALL output columns from matched_pass - no repeated condition logic
standardised AS (
    SELECT
        *,

        -- inferred_value: the standardised numeric result
        CASE matched_pass
            WHEN 'pass_1' THEN numeric_value
            WHEN 'pass_2' THEN converted_value
            WHEN 'pass_3' THEN numeric_value / POWER(10, canonical_exponent)
            ELSE NULL
        END AS inferred_value,

        -- confidence: how confident we are in the inferred value
        CASE matched_pass
            WHEN 'excluded' THEN 'NONE'
            WHEN 'pass_1' THEN
                CASE
                    WHEN is_canonical THEN 'VERY_HIGH'
                    WHEN unit_status = 'known' AND seed_multiply_by = 1 THEN 'HIGH'
                    ELSE 'MEDIUM'
                END
            WHEN 'pass_2' THEN 'HIGH'
            WHEN 'pass_3' THEN 'LOW'
            ELSE 'NONE'
        END AS confidence,

        -- inferred_unit: the standardised unit code (all successful passes target the canonical unit)
        CASE
            WHEN matched_pass IN ('excluded', 'no_match') THEN NULL
            ELSE canonical_unit_code
        END AS inferred_unit,

        -- value_was_converted: TRUE if the numeric value was mathematically changed
        CASE matched_pass
            WHEN 'pass_1' THEN FALSE
            WHEN 'pass_2' THEN TRUE
            WHEN 'pass_3' THEN TRUE
            ELSE FALSE
        END AS value_was_converted,

        -- conversion_reason: human-readable explanation of action taken
        CASE matched_pass
            WHEN 'excluded' THEN 'Excluded unit on this measurement type'
            WHEN 'pass_1' THEN
                CASE
                    WHEN is_canonical THEN 'No value or unit conversion necessary'
                    WHEN unit_status = 'known' AND seed_multiply_by = 1 THEN 'No value conversion, unit was non-standard but numerically equivalent'
                    ELSE 'No value conversion, unit was assumed to be incorrect'
                END
            WHEN 'pass_2' THEN 'Value converted using known unit factor (x' || seed_multiply_by::VARCHAR || ')'
            WHEN 'pass_3' THEN 'Value divided by 10^' || canonical_exponent::VARCHAR || ' (inferred raw base-unit recording)'
            ELSE 'Value out of range after all conversion attempts'
        END AS conversion_reason

    FROM pass_assigned
)

{% endmacro %}
