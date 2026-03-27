{% macro get_observations(cluster_ids, source=none, include_history=false) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Join pre-mapped observations with cluster definitions for flexibility
    WITH cluster_codes AS (
        SELECT DISTINCT
            code as mapped_concept_code,
            cluster_id,
            cluster_description,
            code_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE UPPER(cluster_id) IN ({{ cluster_ids|upper }})
        {% if source %}
          AND source = '{{ source }}'
        {% endif %}
    ){% if include_history %},

    -- Expand cluster codes with historical predecessors from SCT_History
    -- Picks up retired SNOMED codes that map to current cluster codes
    historical_codes AS (
        SELECT DISTINCT
            h.old_concept_id::VARCHAR AS mapped_concept_code,
            cc.cluster_id,
            cc.cluster_description,
            cc.code_description
        FROM {{ ref('stg_nhsd_snomed_sct_history') }} h
        INNER JOIN cluster_codes cc
            ON h.new_concept_id::VARCHAR = cc.mapped_concept_code
        WHERE h.old_concept_id::VARCHAR NOT IN (
            SELECT mapped_concept_code FROM cluster_codes
        )
    ),

    expanded_codes AS (
        SELECT * FROM cluster_codes
        UNION ALL
        SELECT * FROM historical_codes
    ){% endif %}

    SELECT
        o.id,
        o.patient_id,
        o.person_id,
        -- Use date_recorded as fallback if clinical_effective_date is after it (data quality fix)
        CASE
            WHEN o.clinical_effective_date > o.date_recorded THEN o.date_recorded
            ELSE COALESCE(o.clinical_effective_date, '1900-01-01')
        END AS clinical_effective_date,
        o.clinical_effective_date AS clinical_effective_date_raw,  -- Original value for audit
        o.date_recorded,
        o.result_value,
        o.result_value_units_concept_id,
        o.result_unit_code,
        o.result_unit_display,
        o.result_text,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.mapped_concept_id,
        o.mapped_concept_code,
        o.mapped_concept_display,
        o.age_at_event,
        o.lds_start_date_time,
        cc.cluster_id,
        cc.cluster_description,
        cc.code_description
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {% if include_history %}expanded_codes{% else %}cluster_codes{% endif %} cc
        ON o.mapped_concept_code = cc.mapped_concept_code
    -- Deduplicate: preserve legitimate cross-cluster duplicates but remove within-cluster duplicates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.id, cc.cluster_id
        ORDER BY o.mapped_concept_code
    ) = 1

{% endmacro %}
