{{
    config(
        materialized = 'table',
        schema = 'LSPLCM',
        tags = ['sdl', 'slam', 'lsplcm']
    )
}}

-- Staging model for the SDL LSPLCM feed (SLAM PLD: provider patient-level
-- contract-monitoring submissions, spreadsheet/CSV uploads remapped via the
-- servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSPLCM — ~800M rows,
-- 580+ cols (superset of every historic layout), all TEXT.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped; raw values
-- for cleaned fields remain in the source/raw layer.
--
-- What's done:
--   * dv_financial_year / dv_financial_month: provider free text validated to
--     'YYYYYY' (e.g. '202627') and 1-12. Invalid values (e.g. '215551',
--     misaligned rows) -> NULL, with the file-name FY token
--     ('PLCM_2627_InformationStandard...') as fallback. Raw values retained.
--   * dv_dataset_created_at: DATE_AND_TIME_DATA_SET_CREATED multi-format
--     parsed (>99.99% of populated values; ISO-only parsing got ~96%).
--   * Costs/activity to NUMBER(38,6): strips currency symbols, thousands
--     commas, accounting negatives. 'TBC' etc -> NULL.
--   * dv_is_latest_submission: latest provider statement of each
--     (provider, FY, month) slice. SLAM files are cumulative-YTD restatements,
--     so this is flagged per month-slice, ordered by the ISL load time from
--     META_FILE_REGISTRY (file_id is NOT monotonic with load time, and
--     DATE_AND_TIME_DATA_SET_CREATED is provider-typed and unreliable —
--     ~10% of files contain more than one value). A submission split across
--     files (e.g. M1_6 + M7_12) resolves correctly because the flag is
--     per month-slice, not per file.

with registry as (
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
    where feed = 'LSPLCM'
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
        s.local_sub_specialty_code              as local_sub_specialty_code,
        s.service_code                          as service_code,
        s.tariff_code                           as tariff_code,
        s.national_tariff_indicator             as national_tariff_indicator,
        s.reporting_type_indicator              as reporting_type_indicator,
        s.unbundled_episode_indicator           as unbundled_episode_indicator,
        s.local_contract_code                   as local_contract_code,
        s.local_contract_monitoring_code        as local_contract_monitoring_code,
        s.local_contract_monitoring_description as local_contract_monitoring_description,
        s.cam_assignment                        as cam_assignment,

        -- Activity
        {{ parse_uk_date('s.activity_start_date_contract_monitoring') }}
                                                as activity_start_date,
        {{ parse_uk_date('s.activity_end_date_contract_monitoring') }}
                                                as activity_end_date,
        try_to_number(s.adjusted_length_of_stay)
                                                as adjusted_length_of_stay,

        -- Linkage identifiers
        s.cds_unique_identifier                 as cds_unique_identifier,
        s.non_cds_unique_identifier             as non_cds_unique_identifier,
        s.out_patient_attendance_identifier     as out_patient_attendance_identifier,
        s.hospital_provider_spell_identifier    as hospital_provider_spell_identifier,

        -- Measures
        {{ parse_slam_number('s.activity_count_point_of_delivery') }}
                                                as dv_activity_count,
        {{ parse_slam_number('s.activity_unit_price') }}
                                                as dv_activity_unit_price,
        {{ parse_slam_number('s.total_cost') }} as dv_total_cost

    from {{ source('sdl_wnl', 'LSPLCM') }} as s
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
