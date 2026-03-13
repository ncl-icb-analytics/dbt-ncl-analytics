{% test has_new_files(model, timestamp_col='derSubmissionDateTimeFromDLP', lookback_hours=24) %}
-- This test fails when there are no files in the target relation with a timestamp within the last `lookback_hours`.
-- It first checks that the relation exists; if it does not, the test will return a descriptive failing row.
{%- set relation = adapter.get_relation(model.database, model.schema, model.identifier) -%}
{%- if not relation -%}
select
    'relation_missing' as issue,
    '{{ model.database }}' as database,
    '{{ model.schema }}' as schema,
    '{{ model.identifier }}' as identifier
where 1 = 1
{%- else -%}
select 'no_new_files' as issue
where not exists (
        select 1
        from {{ relation }}
        where {{ timestamp_col }} >= dateadd(hour, -{{ lookback_hours }}, current_timestamp())
)
{%- endif -%}
{% endtest %}
