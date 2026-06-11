{{
    config(
        materialized = 'table',
        schema = 'LSACM',
        tags = ['sdl', 'slam', 'lsacm']
    )
}}

-- Staging model for the SDL LSACM feed (SLAM aggregate contract monitoring:
-- provider plan/actual activity and price by POD, spreadsheet/CSV uploads
-- remapped via the servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSACM —
-- ~600M rows, all TEXT. Aggregate feed: no patient-level fields.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped.
-- Cleaning identical to stg_lsplcm — see that model for the full rationale:
-- dv_financial_year/month validation with file-name FY fallback, multi-format
-- dv_dataset_created_at parsing, numeric measures, and dv_is_latest_submission
-- per (provider, FY, month) slice ordered by ISL load time.

with registry as (
    select
        file_id,
        batch_id,
        created_datetime                        as submission_loaded_at,
        original_file_name                      as submission_file_name,
        case
            when try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 3, 2))
                 = mod(try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 1, 2)) + 1, 100)
                then '20' || regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1)
        end                                     as financial_year_from_file_name
    from {{ source('sdl_wnl', 'META_FILE_REGISTRY') }}
    where feed = 'LSACM'
),

src as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        s.meta_sk_row_id::number(38,0)          as meta_sk_row_id,
        s.meta_file_id::number(38,0)            as meta_file_id,
        s.meta_row_id::number(38,0)             as meta_row_id,
        s.meta_batch_id::number(38,0)           as meta_batch_id,
        s.meta_partition_date::date             as meta_partition_date,
        s.meta_provider_code                    as meta_provider_code,
        s.meta_recipient_code                   as meta_recipient_code,
        s.meta_version_id                       as meta_version_id,

        -- Submission identity (from META_FILE_REGISTRY; ISL processing log)
        r.submission_loaded_at                  as submission_loaded_at,
        r.submission_file_name                  as submission_file_name,

        -- Period
        s.financial_year                        as financial_year_raw,
        s.financial_month                       as financial_month_raw,
        coalesce(
            {{ parse_slam_financial_year('s.financial_year') }},
            r.financial_year_from_file_name
        )                                       as dv_financial_year,
        {{ parse_slam_financial_month('s.financial_month') }}
                                                as dv_financial_month,
        s.date_and_time_data_set_created        as dataset_created_raw,
        {{ parse_slam_timestamp('s.date_and_time_data_set_created') }}
                                                as dv_dataset_created_at,

        -- Organisation
        s.organisation_identifier_code_of_provider
                                                as provider_code,
        {{ clean_organisation_id('upper(trim(coalesce(s.organisation_identifier_code_of_provider, s.meta_provider_code)))') }}
                                                as dv_provider_code,
        s.organisation_identifier_code_of_commissioner
                                                as commissioner_code,
        s.organisation_identifier_residence_responsibility
                                                as residence_responsibility_code,
        s.organisation_identifier_gp_practice_responsibility
                                                as gp_practice_responsibility_code,
        s.general_medical_practice_code_patient_registration
                                                as gp_practice_code_registration,
        s.organisation_site_identifier_of_treatment
                                                as site_of_treatment_code,

        -- Service / contract categorisation
        s.commissioned_service_category_code    as commissioned_service_category_code,
        s.point_of_delivery_code                as point_of_delivery_code,
        s.local_point_of_delivery_code          as local_point_of_delivery_code,
        s.local_point_of_delivery_description   as local_point_of_delivery_description,
        s.point_of_delivery_further_detail_code as point_of_delivery_further_detail_code,
        s.point_of_delivery_further_detail_description
                                                as point_of_delivery_further_detail_description,
        s.activity_treatment_function_code      as activity_treatment_function_code,
        s.local_sub_specialty_code              as local_sub_specialty_code,
        s.service_code                          as service_code,
        s.tariff_code                           as tariff_code,
        s.national_tariff_indicator             as national_tariff_indicator,
        s.reporting_type_indicator              as reporting_type_indicator,
        s.local_contract_code                   as local_contract_code,
        s.local_contract_monitoring_code        as local_contract_monitoring_code,
        s.local_contract_monitoring_description as local_contract_monitoring_description,
        s.cam_assignment                        as cam_assignment,

        -- Measures (plan vs actual)
        {{ parse_slam_number('s.contract_monitoring_planned_activity') }}
                                                as dv_planned_activity,
        {{ parse_slam_number('s.contract_monitoring_actual_activity') }}
                                                as dv_actual_activity,
        {{ parse_slam_number('s.contract_monitoring_planned_price') }}
                                                as dv_planned_price,
        {{ parse_slam_number('s.contract_monitoring_actual_price') }}
                                                as dv_actual_price,
        {{ parse_slam_number('s.contract_monitoring_planned_market_forces_factor') }}
                                                as dv_planned_market_forces_factor,
        {{ parse_slam_number('s.contract_monitoring_actual_market_forces_factor') }}
                                                as dv_actual_market_forces_factor

    from {{ source('sdl_wnl', 'LSACM') }} as s
    left join registry as r
        on r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
)

select
    *,
    rank() over (
        partition by dv_provider_code, dv_financial_year, dv_financial_month
        order by submission_loaded_at desc nulls last, meta_file_id desc, meta_batch_id desc
    ) = 1                                       as dv_is_latest_submission
from src
