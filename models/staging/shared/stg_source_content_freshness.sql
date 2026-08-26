/*
One source-derived content-currency signal per profiled DATA_LAKE source.

Dates inside each feed take precedence over Snowflake object-change metadata.
Expected days describe normal delivery cadence. SLA days are contractual and
apply only to OLIDS. Breach days mark a sustained delay that needs attention.
*/

with olids as (
    select
        'DATA_LAKE.OLIDS' as source_schema,
        max(consensus_activity_date)::date as content_date,
        max(max_lds_transform_datetime)::timestamp_ntz as observed_at,
        'practice_consensus_date' as signal_type,
        'Latest activity date reported by the practice consensus' as signal_detail,
        1 as expected_days,
        5 as sla_days,
        10 as breach_after_days
    from {{ ref('raw_olids_freshness_summary') }}
),

pds_core_dates as (
    select
        max(
            case
                when person_business_effective_from_date::date <= current_date()
                    then person_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_person') }}

    union all

    select
        max(
            case
                when usual_address_business_effective_from_date::date <= current_date()
                    then usual_address_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_address') }}

    union all

    select
        max(
            case
                when primary_care_provider_business_effective_from_date::date <= current_date()
                    then primary_care_provider_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_patient_care_practice') }}
),

pds as (
    select
        'DATA_LAKE.PDS' as source_schema,
        case when count(content_at) = 3 then min(content_at)::date end as content_date,
        max(content_at)::timestamp_ntz as observed_at,
        'core_business_effective_date' as signal_type,
        'Latest business-effective date reached by person, address, and registered-practice data' as signal_detail,
        1 as expected_days,
        null::number as sla_days,
        7 as breach_after_days
    from pds_core_dates
),

deaths as (
    select
        'DATA_LAKE.DEATHS' as source_schema,
        max(case when reg_date::date <= current_date() then reg_date end)::date as content_date,
        max(dmic_record_valid_date_from)::timestamp_ntz as observed_at,
        'latest_registration_date' as signal_type,
        'Latest death registration date present in the feed' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
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
        7 as expected_days,
        null::number as sla_days,
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
        7 as expected_days,
        null::number as sla_days,
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
        7 as expected_days,
        null::number as sla_days,
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
        30 as expected_days,
        null::number as sla_days,
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
        30 as expected_days,
        null::number as sla_days,
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
        30 as expected_days,
        null::number as sla_days,
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
        7 as expected_days,
        null::number as sla_days,
        14 as breach_after_days
    from {{ ref('raw_wl_wl_submissionlog_data') }}
    where der_is_latest_filetype_provider_weekending = true
),

ers as (
    select
        'DATA_LAKE.ERS' as source_schema,
        max(case when rp_end_date::date <= current_date() then rp_end_date end)::date as content_date,
        max(dmic_date_added)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest e-Referral Service reporting-period end date in the submission header' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from {{ ref('raw_ers_pc_ebsx00header') }}
),

fact_patient as (
    select
        'DATA_LAKE.FACT_PATIENT' as source_schema,
        max(case when period::date <= current_date() then last_day(period::date) end)::date as content_date,
        null::timestamp_ntz as observed_at,
        'latest_activity_period' as signal_type,
        'Latest monthly activity period; the source table does not expose a load timestamp' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('raw_fact_patient_factactivity') }}
),

pmct as (
    select
        'DATA_LAKE.PMCT' as source_schema,
        last_day(max(
            case
                when try_to_date(
                    left(split_part(period, '-', 2), 3) || ' ' || split_part(period, '-', 3),
                    'MON YYYY'
                ) <= current_date()
                    then try_to_date(
                        left(split_part(period, '-', 2), 3) || ' ' || split_part(period, '-', 3),
                        'MON YYYY'
                    )
            end
        )) as content_date,
        max(create_ts)::timestamp_ntz as observed_at,
        'diagnostics_reporting_period' as signal_type,
        'Latest diagnostics reporting month used by project models' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('raw_pmct_diagnosticsmonthlysourceappendreviseprovcomm') }}
),

tat as (
    select
        'DATA_LAKE.TAT' as source_schema,
        last_day(max(
            case
                when try_to_date(month, 'MON YYYY') <= current_date()
                    then try_to_date(month, 'MON YYYY')
            end
        )) as content_date,
        max(loaded_at)::timestamp_ntz as observed_at,
        'diagnostic_reporting_month' as signal_type,
        'Latest diagnostic turnaround-time reporting month in provider submissions' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('raw_tat_turnaround_times_raw') }}
),

terminology as (
    select
        'DATA_LAKE.TERMINOLOGY' as source_schema,
        max(case when release_date::date <= current_date() then release_date end)::date as content_date,
        max(run_end)::timestamp_ntz as observed_at,
        'latest_successful_release' as signal_type,
        'Latest terminology release loaded successfully from NHS TRUD' as signal_detail,
        35 as expected_days,
        null::number as sla_days,
        120 as breach_after_days
    from {{ ref('raw_reference_ingest_log') }}
    where lower(status) = 'success'
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
union all
select * from ers
union all
select * from fact_patient
union all
select * from pmct
union all
select * from tat
union all
select * from terminology
