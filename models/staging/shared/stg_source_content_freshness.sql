/*
One content-currency signal per source used by the semantic views.

Dates inside each feed take precedence over Snowflake object-change metadata.
PDS has no exposed extract or reporting-period field, so consumers must use the
platform source monitor for that row.
Fresh/warning/breach thresholds follow source cadence: OLIDS 5/10 days,
weekly SUS and waiting-list feeds 7/14 days, and monthly feeds 30/45 days.
*/

with olids as (
    select
        'DATA_LAKE.OLIDS' as source_schema,
        max(consensus_activity_date)::date as content_date,
        max(max_lds_transform_datetime)::timestamp_ntz as observed_at,
        'practice_consensus_date' as signal_type,
        'Latest activity date reported by the practice consensus' as signal_detail,
        false as uses_monitor_fallback,
        5 as sla_days,
        10 as breach_after_days
    from {{ ref('raw_olids_freshness_summary') }}
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
        max(case when reg_date::date <= current_date() then reg_date end)::date as content_date,
        max(dmic_record_valid_date_from)::timestamp_ntz as observed_at,
        'latest_registration_date' as signal_type,
        'Latest death registration date present in the feed' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ ref('raw_registries_deaths_deaths') }}
),

sus_apc as (
    select
        'DATA_LAKE.SUS_UNIFIED_APC' as source_schema,
        max(
            case
                when system_interchange_received_date::date <= current_date()
                    then system_interchange_received_date
            end
        )::date as content_date,
        max(system_report_query_date)::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in admitted-patient records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ ref('raw_sus_apc_spell') }}
),

sus_op as (
    select
        'DATA_LAKE.SUS_UNIFIED_OP' as source_schema,
        max(
            case
                when system_interchange_received_date::date <= current_date()
                    then system_interchange_received_date
            end
        )::date as content_date,
        max(system_report_query_date)::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in outpatient records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ ref('raw_sus_op_appointment') }}
),

sus_ecds as (
    select
        'DATA_LAKE.SUS_UNIFIED_ECDS' as source_schema,
        max(
            case
                when system_interchange_received_date::date <= current_date()
                    then system_interchange_received_date
            end
        )::date as content_date,
        max(system_report_query_date)::timestamp_ntz as observed_at,
        'latest_record_received_date' as signal_type,
        'Latest source receipt date present in emergency-care records' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ ref('raw_sus_ecds_emergency_care') }}
),

epd as (
    select
        'DATA_LAKE.EPD_PRIMARY_CARE' as source_schema,
        max(case when rp_end_date::date <= current_date() then rp_end_date end)::date as content_date,
        max(received_date)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest prescribing reporting-period end date in the submission header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ ref('raw_epd_pc_medsheader') }}
),

csds as (
    select
        'DATA_LAKE.CSDS' as source_schema,
        max(
            case
                when reporting_period_end_date::date <= current_date()
                    then reporting_period_end_date
            end
        )::date as content_date,
        max(upload_date_time)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the community-services header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ ref('raw_csds_cyp000header') }}
),

mhsds as (
    select
        'DATA_LAKE.MHSDS' as source_schema,
        max(
            case
                when reporting_period_end_date::date <= current_date()
                    then reporting_period_end_date
            end
        )::date as content_date,
        max(dmic_date_added)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the mental-health header' as signal_detail,
        false as uses_monitor_fallback,
        30 as sla_days,
        45 as breach_after_days
    from {{ ref('raw_mhsds_mhs000header') }}
),

waiting_list as (
    select
        'DATA_LAKE.WL' as source_schema,
        max(
            case
                when der_week_ending::date <= current_date()
                    then der_week_ending
            end
        )::date as content_date,
        max(der_submission_date_time_from_dlp)::timestamp_ntz as observed_at,
        'week_ending_date' as signal_type,
        'Latest submitted waiting-list week ending date' as signal_detail,
        false as uses_monitor_fallback,
        7 as sla_days,
        14 as breach_after_days
    from {{ ref('raw_wl_wl_submissionlog_data') }}
    where der_is_latest_filetype_provider_weekending = true
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
