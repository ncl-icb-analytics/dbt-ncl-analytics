-- Provides the legacy entry point for deriving SUS OP POD level 4.
{% macro update_pod_op(op_hrg, main_spec) %}
    {{ sus_op_pod_level_4_from_values(op_hrg, main_spec) }}
{% endmacro %}
