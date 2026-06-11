{# Shared macros for the SDL SLAM contract-monitoring feeds (LSACM, LSPLCM,
   LSDrPLCM, LSDePLCM): value parsers for provider-typed free text (formats
   observed in profiling, 2026-06), the submission-slice CTEs that derive
   dv_is_latest_submission, and the column blocks common to all 4 staging
   models. Models supply only feed-specific columns; aliases are fixed:
   `s` = source table, `sl` = submission_slices_ranked. #}

{# Money/activity string -> NUMBER(38,6).
   Handles thousands commas, currency symbols (incl. mojibake bytes), spaces,
   and accounting-style "(1,234.56)" negatives. 'TBC' and other non-numeric
   text -> NULL. #}
{% macro parse_slam_number(col) %}
    case
        when trim({{ col }}) rlike '^\\(.*\\)$'
            then -1 * try_to_number(regexp_replace({{ col }}, '[^0-9.]', ''), 38, 6)
        else try_to_number(regexp_replace({{ col }}, '[^0-9.-]', ''), 38, 6)
    end
{% endmacro %}

{# Provider-typed timestamp string -> TIMESTAMP_NTZ. Each branch is regex-gated
   so only well-formed inputs reach each parser. Observed formats:
     ISO (also with broken ' 00:00' suffixes), YYYY/MM/DD HH:MI,
     US MM/DD/YYYY AM/PM (SQL Server export), UK DD/MM/YYYY and DD-MM-YYYY
     with/without time, DD-Mon-YY(YY), Excel serial day numbers.
   Unparseable junk (month-only '2020-10 18:39', 'mm:ss.s' artefacts) -> NULL. #}
{% macro parse_slam_timestamp(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then coalesce(try_to_timestamp({{ col }}), try_to_timestamp(left({{ col }}, 19), 'YYYY-MM-DD HH24:MI:SS'))
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{1,2}:[0-9]{2}.*'
            then coalesce(try_to_timestamp({{ col }}), try_to_timestamp(left({{ col }}, 16), 'YYYY-MM-DD HH24:MI'))
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then try_to_timestamp({{ col }}, 'YYYY-MM-DD')
        when {{ col }} rlike '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}[ ][0-9]{1,2}:[0-9]{2}$'
            then try_to_timestamp({{ col }}, 'YYYY/MM/DD HH24:MI')
        when upper({{ col }}) rlike '.*[ ](AM|PM)$'
            then try_to_timestamp({{ col }}, 'MM/DD/YYYY HH12:MI:SS AM')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}$'
            then try_to_timestamp({{ col }} || ':00', 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD-MM-YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$'
            then try_to_timestamp({{ col }}, 'DD-MM-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_timestamp({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_timestamp({{ col }}, 'DD-MON-YY')
        when {{ col }} rlike '^4[0-9]{4}(\\.[0-9]+)?$'
            then dateadd('second', round((try_to_number({{ col }}, 18, 6) - 25569) * 86400), '1970-01-01'::timestamp)
    end
{% endmacro %}

{# Financial year string -> canonical 'YYYYYY' (e.g. '202526'), validated so the
   second year follows the first. Accepts '202526', '2025/26', '2025-26',
   '2025 26', '2025-2026'. Bare calendar years ('2020') are ambiguous -> NULL.
   Garbage ('215551', specialty names on misaligned rows) -> NULL. #}
{% macro parse_slam_financial_year(col) %}
    case
        when regexp_replace({{ col }}, '[^0-9]', '') rlike '^20[0-9]{4}$'
             and try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 5, 2))
                 = mod(try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 3, 2)) + 1, 100)
            then regexp_replace({{ col }}, '[^0-9]', '')
        when regexp_replace({{ col }}, '[^0-9]', '') rlike '^20[0-9]{6}$'
             and try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 5, 4))
                 = try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 1, 4)) + 1
            then substr(regexp_replace({{ col }}, '[^0-9]', ''), 1, 4)
                 || substr(regexp_replace({{ col }}, '[^0-9]', ''), 7, 2)
    end
{% endmacro %}

{# Financial month -> INT 1-12, else NULL (junk like '110', '400' on
   misaligned rows). Whole numbers only: TRY_TO_NUMBER at scale 0 would
   silently round fractional values (e.g. '5.5' -> 6). #}
{% macro parse_slam_financial_month(col) %}
    case
        when trim(cast({{ col }} as varchar)) rlike '^[0-9]{1,2}$'
             and try_to_number(trim(cast({{ col }} as varchar))) between 1 and 12
            then cast(try_to_number(trim(cast({{ col }} as varchar))) as int)
    end
{% endmacro %}

{# Emits the submission-slice CTEs shared by the 4 SLAM staging models:
     registry                    - META_FILE_REGISTRY for the feed, with the
                                   file-name FY token
     submission_slices           - one row per (file, raw FY, raw month, raw
                                   provider) combination, aggregated straight
                                   off the source table (~10-100k rows vs
                                   600-800M)
     submission_slices_enriched  - slice rows with parsed dv fields and the
                                   registry submission identity
   Computing the regex/dictionary-lookup dv fields at slice grain then
   hash-joining back keeps them off the full-table pass. Latest-submission
   resolution lives in stg_slam_latest_submission (slice grain, rebuilt fully
   each run) — not here, so the staging tables stay pure append.
   `table_name` is the source table; `feed` is the registry FEED literal
   (mixed case, e.g. 'LSDrPLCM'). #}
{% macro slam_submission_slices(table_name, feed) %}
registry as (
    select
        file_id,
        batch_id,
        created_datetime                        as submission_loaded_at,
        original_file_name                      as submission_file_name,
        -- FY token from the platform-generated file-name prefix,
        -- e.g. 'PLCM_2627_InformationStandard...' -> '202627'
        case
            when try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 3, 2))
                 = mod(try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 1, 2)) + 1, 100)
                then '20' || regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1)
        end                                     as financial_year_from_file_name
    from {{ source('sdl_wnl', 'META_FILE_REGISTRY') }}
    where feed = '{{ feed }}'
),

submission_slices as (
    select
        meta_file_id,
        meta_batch_id,
        meta_provider_code,
        financial_year,
        financial_month,
        organisation_identifier_code_of_provider as provider_code_raw
    from {{ source('sdl_wnl', table_name) }}
    group by all
),

submission_slices_enriched as (
    select
        s.*,
        r.submission_loaded_at,
        r.submission_file_name,
        coalesce(
            {{ parse_slam_financial_year('s.financial_year') }},
            r.financial_year_from_file_name
        )                                       as dv_financial_year,
        {{ parse_slam_financial_month('s.financial_month') }}
                                                as dv_financial_month,
        {{ clean_organisation_id('upper(trim(coalesce(s.provider_code_raw, s.meta_provider_code)))') }}
                                                as dv_provider_code
    from submission_slices as s
    left join registry as r
        on r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
)
{% endmacro %}

{# Incremental CTE: (file, batch) pairs in source but not yet in {{ this }},
   mirroring the upstream SDL loader's own NOT EXISTS append mechanics —
   meta_file_id is not monotonic with load time and back-dated cascade-matched
   files arrive with old ids, so a high-water mark would miss them.
   Upstream full rebuilds (META_BUILD_STATE.NEEDS_REBUILD, schema drift) can
   delete/rewrite history — recover with dbt build --full-refresh. #}
{% macro slam_incremental_slices(table_name) %}
{% if is_incremental() %}
,
new_files as (
    select meta_file_id, meta_batch_id
    from {{ source('sdl_wnl', table_name) }}
    group by all
    minus
    select distinct meta_file_id, meta_batch_id
    from {{ this }}
)
{% endif %}
{% endmacro %}

{# Row filter for incremental runs: append rows from new files only. #}
{% macro slam_incremental_where() %}
{% if is_incremental() %}
where exists (
    select 1
    from new_files as nf
    where nf.meta_file_id = s.meta_file_id
      and nf.meta_batch_id = s.meta_batch_id
)
{% endif %}
{% endmacro %}

{# Body of a stg_*_latest view: staging rows restricted to the file holding
   the current provider statement of each (provider, FY, month) slice, via
   the stg_slam_latest_submission lookup. #}
{% macro slam_latest_view(staging_model, feed) %}
select s.*
from {{ ref(staging_model) }} as s
join {{ ref('stg_slam_latest_submission') }} as l
    on l.feed = '{{ feed }}'
   and l.meta_file_id = s.meta_file_id
   and l.meta_batch_id = s.meta_batch_id
   and equal_null(l.dv_provider_code, s.dv_provider_code)
   and equal_null(l.dv_financial_year, s.dv_financial_year)
   and equal_null(l.dv_financial_month, s.dv_financial_month)
{% endmacro %}

{# Join condition matching a source row to its submission slice. Null-safe on
   the raw period/provider strings (all nullable). #}
{% macro slam_slice_join() %}
    on sl.meta_file_id = s.meta_file_id
   and sl.meta_batch_id = s.meta_batch_id
   and equal_null(sl.meta_provider_code, s.meta_provider_code)
   and equal_null(sl.financial_year, s.financial_year)
   and equal_null(sl.financial_month, s.financial_month)
   and equal_null(sl.provider_code_raw, s.organisation_identifier_code_of_provider)
{% endmacro %}

{# META keys from the SDL pipeline (fully reliable). #}
{% macro slam_meta_columns() %}
        s.meta_sk_row_id::number(38,0)          as meta_sk_row_id,
        s.meta_file_id::number(38,0)            as meta_file_id,
        s.meta_row_id::number(38,0)             as meta_row_id,
        s.meta_batch_id::number(38,0)           as meta_batch_id,
        s.meta_partition_date::date             as meta_partition_date,
        s.meta_provider_code                    as meta_provider_code,
        s.meta_recipient_code                   as meta_recipient_code,
        s.meta_version_id                       as meta_version_id
{% endmacro %}

{# Submission identity + cleaned reporting period. Latest-submission
   resolution lives in stg_slam_latest_submission / the *_latest views. #}
{% macro slam_submission_columns() %}
        sl.submission_loaded_at                 as submission_loaded_at,
        sl.submission_file_name                 as submission_file_name,
        s.financial_year                        as financial_year_raw,
        s.financial_month                       as financial_month_raw,
        sl.dv_financial_year                    as dv_financial_year,
        sl.dv_financial_month                   as dv_financial_month,
        s.date_and_time_data_set_created        as dataset_created_raw,
        {{ parse_slam_timestamp('s.date_and_time_data_set_created') }}
                                                as dv_dataset_created_at
{% endmacro %}

{# Organisation identifiers common to all 4 feeds. #}
{% macro slam_org_columns() %}
        s.organisation_identifier_code_of_provider
                                                as provider_code,
        sl.dv_provider_code                     as dv_provider_code,
        s.organisation_identifier_code_of_commissioner
                                                as commissioner_code,
        s.organisation_identifier_residence_responsibility
                                                as residence_responsibility_code,
        s.organisation_identifier_gp_practice_responsibility
                                                as gp_practice_responsibility_code,
        s.general_medical_practice_code_patient_registration
                                                as gp_practice_code_registration,
        s.organisation_site_identifier_of_treatment
                                                as site_of_treatment_code
{% endmacro %}

{# Service / contract categorisation common to all 4 feeds. #}
{% macro slam_service_columns() %}
        s.commissioned_service_category_code    as commissioned_service_category_code,
        s.point_of_delivery_code                as point_of_delivery_code,
        s.local_point_of_delivery_code          as local_point_of_delivery_code,
        s.local_point_of_delivery_description   as local_point_of_delivery_description,
        s.point_of_delivery_further_detail_code as point_of_delivery_further_detail_code,
        s.point_of_delivery_further_detail_description
                                                as point_of_delivery_further_detail_description,
        s.activity_treatment_function_code      as activity_treatment_function_code,
        s.service_code                          as service_code,
        s.reporting_type_indicator              as reporting_type_indicator,
        s.local_contract_code                   as local_contract_code,
        s.local_contract_monitoring_code        as local_contract_monitoring_code,
        s.local_contract_monitoring_description as local_contract_monitoring_description,
        s.cam_assignment                        as cam_assignment
{% endmacro %}

{# Patient identifiers/demographics for the patient-level feeds
   (LSPLCM, LSDrPLCM, LSDePLCM; LSACM is aggregate). #}
{% macro slam_patient_columns() %}
        s.sk_patient_id_nhs_number              as sk_patient_id,
        s.local_patient_identifier_extended     as local_patient_identifier,
        try_to_number(s.age_at_activity_date_contract_monitoring)
                                                as age_at_activity_date,
        try_to_number(s.dv_yearof_birth)        as year_of_birth,
        s.person_stated_gender_code             as gender_code,
        s.ethnic_category                       as ethnic_category,
        s.dv_lsoa                               as lsoa,
        s.dv_partial_post_code                  as partial_post_code
{% endmacro %}
