{# Shared building blocks for the community PLD staging models (stg_comopl,
   stg_ref): the identical meta-key casts, file-registry CTE, stated-period
   parse and the financial-period derivation. Keeps the period precedence
   (stated -> activity date -> file name) defined once across both feeds. #}

{# 8 SDL meta keys, cast and aliased. Used in the prep CTE; columns are
   unqualified (the source is the only base table carrying them). #}
{% macro community_pld_meta_columns() %}
        meta_sk_row_id::number(38,0)            as meta_sk_row_id,
        meta_file_id::number(38,0)              as meta_file_id,
        meta_row_id::number(38,0)               as meta_row_id,
        meta_batch_id::number(38,0)             as meta_batch_id,
        meta_partition_date::date               as meta_partition_date,
        meta_provider_code                      as meta_provider_code,
        meta_recipient_code                     as meta_recipient_code,
        meta_version_id                         as meta_version_id
{% endmacro %}

{# META_FILE_REGISTRY rows for one feed; original_file_name feeds the
   period-of-last-resort fallback. Join on (file_id, batch_id). #}
{% macro community_pld_registry(feed) %}
registry as (
    select file_id, batch_id, original_file_name
    from {{ source('sdl_wnl', 'META_FILE_REGISTRY') }}
    where feed = '{{ feed }}'
)
{% endmacro %}

{# Provider-stated reporting period (DLP cols, else plain financial cols),
   validated. The final select coalesces these with the activity-date and
   file-name fallbacks via community_pld_financial_period. #}
{% macro community_pld_stated_period() %}
        {{ parse_slam_financial_month('coalesce(dlp_financial_month, financial_month)') }}
                                                as dv_financial_month_stated,
        {{ fin_year_from_start_year('coalesce(dlp_financial_year, financial_year)') }}
                                                as dv_financial_year_stated
{% endmacro %}

{# Resolved financial period for the final select: stated value, else derived
   from the activity date, else the file-name period (each gated to a plausible
   range so junk cannot create phantom periods). dv_financial_period_source
   records which was used. `activity_date` is the feed's parsed activity-date
   column (in scope from prep); `activity_source` names it in the provenance
   label, e.g. 'contact_date' -> 'derived_from_contact_date'. #}
{% macro community_pld_financial_period(activity_date, activity_source) %}
    -- Year and month resolve from the SAME source (paired precedence) so a row
    -- can't take its year from one source and month from another; the source is
    -- recorded in dv_financial_period_source.
    case
        when dv_financial_month_stated is not null and dv_financial_year_stated is not null
            then dv_financial_year_stated
        when {{ activity_date }} between '2015-04-01' and current_date()
            then {{ fin_year_from_date(activity_date) }}
        when file_name_period between '2015-04-01' and current_date()
            then {{ fin_year_from_date('file_name_period') }}
    end                                         as dv_financial_year,
    case
        when dv_financial_month_stated is not null and dv_financial_year_stated is not null
            then dv_financial_month_stated
        when {{ activity_date }} between '2015-04-01' and current_date()
            then {{ fin_month_from_date(activity_date) }}
        when file_name_period between '2015-04-01' and current_date()
            then {{ fin_month_from_date('file_name_period') }}
    end                                         as dv_financial_month,
    case
        when dv_financial_month_stated is not null and dv_financial_year_stated is not null
            then 'stated'
        when {{ activity_date }} between '2015-04-01' and current_date()
            then 'derived_from_{{ activity_source }}'
        when file_name_period between '2015-04-01' and current_date()
            then 'derived_from_file_name'
    end                                         as dv_financial_period_source
{% endmacro %}
