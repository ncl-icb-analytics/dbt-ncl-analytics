{{
    config(
        materialized = 'table',
        schema = 'LSDEPLCM',
        tags = ['sdl', 'slam', 'lsdeplcm']
    )
}}

-- Staging model for the SDL LSDePLCM feed (SLAM high-cost devices PLD:
-- provider patient-level device contract-monitoring submissions, remapped via
-- the servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSDEPLCM — ~4M rows,
-- all TEXT.
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
    where feed = 'LSDePLCM'
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

        -- Patient
        s.sk_patient_id_nhs_number              as sk_patient_id,
        s.local_patient_identifier_extended     as local_patient_identifier,
        try_to_number(s.age_at_activity_date_contract_monitoring)
                                                as age_at_activity_date,
        try_to_number(s.dv_yearof_birth)        as year_of_birth,
        s.person_stated_gender_code             as gender_code,
        s.ethnic_category                       as ethnic_category,
        s.dv_lsoa                               as lsoa,
        s.dv_partial_post_code                  as partial_post_code,

        -- Service / contract categorisation
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
        s.cam_assignment                        as cam_assignment,
        s.value_added_tax_charged_indicator_contract_monitoring
                                                as vat_charged_indicator,

        -- Device detail
        s.medical_device_name_high_cost_tariff_excluded_device
                                                as device_name,
        s.medical_device_quantity_high_cost_tariff_excluded_device
                                                as device_quantity_raw,
        {{ parse_slam_number('s.medical_device_quantity_high_cost_tariff_excluded_device') }}
                                                as dv_device_quantity,
        s.medical_device_size_high_cost_tariff_excluded_device
                                                as device_size,
        s.medical_device_manufacturer_high_cost_tariff_excluded_device
                                                as device_manufacturer,
        s.medical_device_serial_number_high_cost_tariff_excluded_device
                                                as device_serial_number,
        s.medical_device_procurement_route_high_cost_tariff_excluded_device
                                                as device_procurement_route,
        s.local_code_high_cost_tariff_excluded_device
                                                as local_device_code,
        s.high_level_code_high_cost_tariff_excluded_device
                                                as high_level_device_code,
        s.subsidiary_level_code_high_cost_tariff_excluded_device
                                                as subsidiary_device_code,
        {{ parse_uk_date('s.clinical_intervention_date_medical_device_implementation') }}
                                                as device_implantation_date,

        -- Linkage identifiers
        s.non_cds_unique_identifier             as non_cds_unique_identifier,
        s.out_patient_attendance_identifier     as out_patient_attendance_identifier,
        s.hospital_provider_spell_identifier    as hospital_provider_spell_identifier,

        -- Measures
        {{ parse_slam_number('s.unit_price_commissioner') }}
                                                as dv_unit_price_commissioner,
        {{ parse_slam_number('s.unit_price_supplier') }}
                                                as dv_unit_price_supplier,
        {{ parse_slam_number('s.total_cost') }} as dv_total_cost

    from {{ source('sdl_wnl', 'LSDEPLCM') }} as s
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
