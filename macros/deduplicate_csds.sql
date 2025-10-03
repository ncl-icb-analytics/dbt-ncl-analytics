
{% macro deduplicate_csds(
        csds_table,
        partition_cols = []
        ) %}

    {# detect whether csds_table is the cyp101referral table #}
    {% set is_referral = 'cyp101referral' in csds_table|lower %}

    {% if is_referral %}
        {% set unique_id_col = 'tbl.unique_service_request_identifier' %}
    {% else %}
        {% set unique_id_col = 'referral.unique_service_request_identifier' %}
    {% endif %}

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY {{ unique_id_col }}
            {%- for col in partition_cols %}
                , tbl.{{ col }}
            {%- endfor %}
            ORDER BY tbl.effective_from DESC
        ) AS sequence,
        tbl.* 

    FROM {{csds_table}} AS tbl

    INNER JOIN {{ref('raw_csds_activesubmission')}} AS a
        ON tbl.unique_submission_id = a.unique_submission_id

    {% if not is_referral %}

        INNER JOIN {{ref('raw_csds_cyp101referral')}} AS referral
            ON tbl.record_number = referral.record_number

    {% endif %}
    
    QUALIFY sequence = 1

{% endmacro %}