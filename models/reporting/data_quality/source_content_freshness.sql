{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

/*
One content-currency signal per source used by the semantic views.

Dates inside each feed take precedence over Snowflake object-change metadata.
PDS has no exposed extract or reporting-period field, so consumers must use the
platform source monitor for that row.
*/

with olids as (
    select
        'DATA_LAKE.OLIDS' as source_schema,
        global_data_refresh_date::date as content_date,
        current_timestamp()::timestamp_ntz as observed_at,
        'practice_consensus_date' as signal_type,
        'Latest activity date reported by at least 150 practices' as signal_detail,
        false as uses_monitor_fallback,
        5 as sla_days,
        10 as breach_after_days
    from {{ ref('int_global_data_refresh_date') }}
),

pds as (
    select
        'DATA_LAKE.PDS' as source_schema,
        null::date as content_date,
        null::timestamp_ntz as observed_at,
        'platform_monitor_fallback' as signal_type,
        'No extract or reporting-period field is exposed by the source tables' as signal_detail,
        true as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
),

deaths as (
    select
        'DATA_LAKE.DEATHS' as source_schema,
        max("REG_DATE")::date as content_date,
        max("DMIC_RECORD_VALID_DATE_FROM")::timestamp_ntz as observed_at,
        'latest_registration_date' as signal_type,
        'Latest death registration date present in the feed' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ source('registries_deaths', 'Deaths') }}
),

sus_apc as (
    select
        'DATA_LAKE.SUS_UNIFIED_APC' as source_schema,
        max("system.interchange.received_date")::date as content_date,
        max("system.report.query_date")::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in admitted-patient records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ source('sus_apc', 'spell') }}
),

sus_op as (
    select
        'DATA_LAKE.SUS_UNIFIED_OP' as source_schema,
        max("system.interchange.received_date")::date as content_date,
        max("system.report.query_date")::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in outpatient records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ source('sus_op', 'appointment') }}
),

sus_ecds as (
    select
        'DATA_LAKE.SUS_UNIFIED_ECDS' as source_schema,
        max("system.interchange.received_date")::date as content_date,
        max("system.report.query_date")::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in emergency-care records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ source('sus_ecds', 'emergency_care') }}
),

epd as (
    select
        'DATA_LAKE.EPD_PRIMARY_CARE' as source_schema,
        max("RPEndDate")::date as content_date,
        max("ReceivedDate")::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest prescribing reporting-period end date in the submission header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ source('epd_primary_care', 'MedsHeader') }}
),

csds as (
    select
        'DATA_LAKE.CSDS' as source_schema,
        max("REPORTING PERIOD END DATE")::date as content_date,
        max("UPLOAD DATE TIME")::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the community-services header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ source('csds', 'CYP000Header') }}
),

mhsds as (
    select
        'DATA_LAKE.MHSDS' as source_schema,
        max("ReportingPeriodEndDate")::date as content_date,
        max("dmicDateAdded")::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the mental-health header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ source('mhsds', 'MHS000Header') }}
),

waiting_list as (
    select
        'DATA_LAKE.WL' as source_schema,
        max("derWeekEnding")::date as content_date,
        max("derSubmissionDateTimeFromDLP")::timestamp_ntz as observed_at,
        'week_ending_date' as signal_type,
        'Latest submitted waiting-list week ending date' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ source('wl', 'WL_SubmissionLog_Data') }}
    where "derIsLatestFiletypeProviderWeekending" = true
)

select * from olids
union all
select * from pds
union all
select * from deaths
union all
select * from sus_apc
union all
select * from sus_op
union all
select * from sus_ecds
union all
select * from epd
union all
select * from csds
union all
select * from mhsds
union all
select * from waiting_list
