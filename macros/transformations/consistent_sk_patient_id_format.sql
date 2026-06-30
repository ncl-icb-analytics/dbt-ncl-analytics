{# Coerce a patient surrogate-key expression to VARCHAR. Use for every sk_patient_id
   column in commissioning staging models so a future switch to numeric keys can
   be made in one place by updating this macro. #}
{% macro consistent_sk_patient_id_format(column) %}
cast({{ column }} as varchar)
{% endmacro %}
