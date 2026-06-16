{{
    config(
        materialized = 'table',
        schema = 'COMOPL',
        tags = ['sdl', 'community_pld', 'comopl']
    )
}}

-- Staging model for the SDL COMOPL feed (Community Outpatient / contact-level
-- activity, the source for the Community SLAM PLD - Appendix 5a). Source:
-- DATA_LAKE.SDL.COMOPL — ~94M rows, 620 cols (superset of every historic
-- provider layout), all TEXT.
--
-- Cleaned and projected to the 28-field Appendix 5a spec. Grain 1:1 with
-- source. Output columns follow the spec order; a downstream view can rename
-- to the exact spec field names and restrict as needed.
--
-- Column choice is fill-driven: the spec-named columns are frequently sparse
-- in this superset, so each field coalesces the spec column with the
-- best-populated provider sibling (fill rates profiled 2026-06). Notable cases:
--   * gender: GENDER (56%) carries the data, not PERSON_STATED_GENDER_CODE (0%)
--   * local patient id: LOCAL_PATIENT_ID / LOCAL_PATIENT_IDENTIFIER (~42%),
--     not the spec's LOCAL_PATIENT_IDENTIFIER_EXTENDED (0%)
--   * provider: PROVIDER_CODE (9%) / META_PROVIDER_CODE (100%), not
--     ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER (6%)
--   * contact date / discharge date: coalesced across siblings
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key (SK_PATIENT_ID_NHS_NUMBER), Date of Birth is year only
-- (dv_year_of_birth), Postcode is partial (partial_postcode).
--
-- Financial year here is a bare 4-digit year (e.g. 2019 = FY2019/20), unlike
-- the SLAM feeds' YYYYYY — so dv_financial_year is the validated 4-digit year.

with src as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        meta_sk_row_id::number(38,0)            as meta_sk_row_id,
        meta_file_id::number(38,0)              as meta_file_id,
        meta_row_id::number(38,0)               as meta_row_id,
        meta_batch_id::number(38,0)             as meta_batch_id,
        meta_partition_date::date               as meta_partition_date,
        meta_provider_code                      as meta_provider_code,
        meta_recipient_code                     as meta_recipient_code,
        meta_version_id                         as meta_version_id,

        -- 1-5: DLP standard fields (raw passthrough; only ~56% of rows carry them)
        dlp_flexor_freeze                       as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_financial_month                     as dlp_financial_month,
        dlp_financial_year                      as dlp_financial_year,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Cleaned reporting period (coalesce DLP with plain financial cols)
        {{ parse_slam_financial_month('coalesce(dlp_financial_month, financial_month)') }}
                                                as dv_financial_month,
        case
            when trim(coalesce(dlp_financial_year, financial_year)) rlike '^20[0-9]{2}$'
                then cast(coalesce(dlp_financial_year, financial_year) as int)
        end                                     as dv_financial_year,

        -- 6: Local patient identifier (spec col is empty; siblings carry it)
        coalesce(
            local_patient_identifier_extended,
            local_patient_id,
            local_patient_identifier
        )                                       as local_patient_id,

        -- 7-9: Patient identifiers (pseudonymised)
        sk_patient_id_nhs_number                as sk_patient_id,
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        dv_partial_post_code                    as partial_postcode,

        -- 10: Gender (GENDER carries it, not PERSON_STATED_GENDER_CODE)
        coalesce(person_stated_gender_code, gender)
                                                as gender_code,
        -- 11: Ethnic category
        ethnic_category                         as ethnic_category_code,
        -- 12: Registered GP practice
        coalesce(
            general_medical_practice_code_patient_registration,
            gp_practice_code
        )                                       as gp_practice_code,

        -- 13: Source of referral (genuinely sparse on this contact-level feed)
        coalesce(source_of_referral_for_community, referral_source)
                                                as source_of_referral_code,
        -- 14: Referral request received date
        {{ parse_uk_date('coalesce(referral_date, dateof_referral, referral_received_date)') }}
                                                as referral_received_date,
        -- 15: Referral request received time
        nullif(trim(referral_request_received_time), '')
                                                as referral_received_time,
        -- 16: Service or team type referred to
        coalesce(service_or_team_type_referred_to_community_care, team_type)
                                                as team_type_code,
        -- 17: Priority type
        coalesce(priority_type, priority_code)  as priority_type_code,
        -- 18: Care contact date
        {{ parse_uk_date('coalesce(care_contact_date, contact_date)') }}
                                                as contact_date,
        -- 19: Care contact time
        nullif(trim(care_contact_time), '')     as contact_time,
        -- 20: Care contact cancellation reason
        care_contact_cancellation_reason        as contact_cancellation_reason,
        -- 21: Consultation type
        consultation_type                       as consultation_type_code,
        -- 22: Consultation mechanism
        consultation_medium_used                as consultation_mechanism_code,
        -- 23: Attendance status
        attendance_status                       as attendance_status_code,
        -- 24: Service discharge date
        {{ parse_uk_date('coalesce(service_discharge_date, discharge_date, date_dischargedfrom_caseload, referral_closure_date)') }}
                                                as discharge_date,

        -- 25: Provider (cleaned ODS code; provider col sparse, fall back to meta)
        {{ clean_organisation_id('upper(trim(coalesce(organisation_identifier_code_of_provider, provider_code, meta_provider_code)))') }}
                                                as provider_code,
        -- 26: Service reporting line
        service_reporting_line                  as service_reporting_line,
        -- 27: Service POD
        service_pod                             as service_pod,
        -- 28: Service request identifier
        service_request_identifier              as service_request_id,

        -- Raw period values retained for traceability
        coalesce(dlp_financial_year, financial_year)    as financial_year_raw,
        coalesce(dlp_financial_month, financial_month)  as financial_month_raw

    from {{ source('sdl_wnl', 'COMOPL') }}
)

select * from src
