{{
    config(
        materialized = 'table',
        schema = 'REF',
        tags = ['sdl', 'community_pld', 'ref']
    )
}}

-- Staging model for the SDL REF feed (Community Referrals, the source for the
-- Community Referrals PLD - Appendix 5b). Source: DATA_LAKE.SDL.REF — ~50M
-- rows, 377 cols (superset of every historic provider layout), all TEXT.
--
-- Cleaned and projected to the 20-field Appendix 5b spec. Grain 1:1 with
-- source. Output columns follow the spec order; a downstream view can rename
-- to the exact spec field names and restrict as needed.
--
-- Coded fields are normalised to their national code set where recognisable
-- (gender/priority word maps, zero-padding of leading-zero-stripped numerics,
-- free-text ethnicity via the shared nhs_ethnicity_category_code macro proven
-- on stg_mhcorl); genuine provider-local codes pass through unchanged.
-- Validated 2026-06 against the Appendix 5b NationalCodes_* sets: gender/ethnic
-- 100%, priority 99.6%, source-of-referral / reason ~67-69%. team_type is ~8%
-- conformant because most providers submit local team codes here (e.g.
-- HOUCID12, 239496), not the national set — needs a provider-local lookup.
--
-- Financial period (dv_financial_year/month) uses the provider-stated value,
-- falling back to the referral received date when absent (the SLAM #806
-- lesson): lifts coverage ~44% -> ~97%. dv_financial_period_source records
-- which was used. GP practice is cleaned to a valid 6-char ODS code.
--
-- Column choice is fill-driven (rates profiled 2026-06). Notable cases:
--   * NHS Number (pseudo): SK_PATIENT_ID_NHS_NUMBER (82%), not the spec-aligned
--     SK_PATIENT_ID_NHSNUMBER (7%)
--   * gender: GENDER (64%), not PERSON_STATED_GENDER_CODE (0%)
--   * provider: PROVIDER_CODE (65%) / META_PROVIDER_CODE (100%), not
--     ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER (6%)
--   * source of referral: REFERRAL_SOURCE_CODE (62%)
--   * referral reason / team type: coalesced across siblings
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key, Date of Birth is year only, Postcode is partial.
--
-- Financial year here is a bare 4-digit year (e.g. 2019 = FY2019/20).

with prep as (
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

        -- 1-5: DLP standard fields (~44% of rows carry them)
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_financial_month                     as dlp_financial_month,
        dlp_financial_year                      as dlp_financial_year,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period (~44% of rows); the activity-date
        -- fallback below recovers the rest.
        {{ parse_slam_financial_month('coalesce(dlp_financial_month, financial_month)') }}
                                                as dv_financial_month_stated,
        case
            when trim(coalesce(dlp_financial_year, financial_year)) rlike '^20[0-9]{2}$'
                then cast(coalesce(dlp_financial_year, financial_year) as int)
        end                                     as dv_financial_year_stated,

        -- 6: Service request identifier (coalesce referral id siblings)
        coalesce(
            service_request_identifier,
            local_referral_identifier,
            referral_identifier
        )                                       as service_request_id,
        -- 7: Local patient identifier (spec col empty; siblings carry it)
        coalesce(
            local_patient_identifier_extended,
            local_patient_id,
            local_patient_identifier,
            localpatientid
        )                                       as local_patient_id,
        -- 8-10: Patient identifiers (pseudonymised)
        coalesce(
            sk_patient_id_nhs_number,
            sk_patient_id_nhsnumber,
            sk_patient_id_nhs_no
        )                                       as sk_patient_id,
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        dv_partial_post_code                    as partial_postcode,

        -- 11: Gender -> national code (GENDER carries it; maps M/F/words)
        {{ nc_gender('coalesce(person_stated_gender_code, gender)') }}
                                                as gender_code,
        -- 12: Ethnic category -> NHS national code (maps free-text variants)
        {{ nhs_ethnicity_category_code('coalesce(ethnic_category, ethnicity)') }}
                                                as ethnic_category_code,
        -- 13: Registered GP practice (ODS 6-char: 1 letter + 5 digits)
        case
            when coalesce(general_medical_practice_code_patient_registration, general_practice_patient_registration)
                rlike '^[A-Za-z][0-9]{5}'
            then upper(left(coalesce(general_medical_practice_code_patient_registration, general_practice_patient_registration), 6))
        end                                     as gp_practice_code,
        -- 14: Source of referral (zero-pad national 01-99; local codes pass through)
        {{ nc_pad('coalesce(referral_source_code, source_of_referral, source_of_referral_code)', 2) }}
                                                as source_of_referral_code,
        -- 15: Referral request received date
        {{ parse_uk_date('referral_request_received_date') }}
                                                as referral_received_date,
        -- 16: Service or team type referred to (mostly provider-local codes on
        -- this feed; zero-pad the national-coded minority, local pass through)
        {{ nc_pad('coalesce(service_type_requested_code, team_referred_to_code)', 2) }}
                                                as team_type_code,
        -- 17: Priority type -> national code (1 Routine, 2 Urgent, 3 TWW)
        {{ nc_priority('coalesce(priority_type_code, referral_priority)') }}
                                                as priority_type_code,
        -- 18: Primary reason for referral (zero-pad national 001-090; local pass through)
        {{ nc_pad('coalesce(reason_for_referral_code, referral_reason_code, primary_referral_reason)', 3) }}
                                                as primary_referral_reason_code,
        -- 19: Service reporting line
        service_reporting_line                  as service_reporting_line,
        -- 20: Provider (cleaned ODS code)
        {{ clean_organisation_id('upper(trim(coalesce(organisation_identifier_code_of_provider, provider_code, meta_provider_code)))') }}
                                                as provider_code,

        -- Raw period values retained for traceability
        coalesce(dlp_financial_year, financial_year)    as financial_year_raw,
        coalesce(dlp_financial_month, financial_month)  as financial_month_raw

    from {{ source('sdl_wnl', 'REF') }}
)

-- Final projection. Derived reporting period sits up front (after the meta
-- keys); financial period is the stated value, else derived from the referral
-- received date (gated to a plausible range), with dv_financial_period_source
-- recording which was used.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived)
    coalesce(
        dv_financial_year_stated,
        case when referral_received_date between '2015-04-01' and current_date()
             then {{ fin_year_start_from_date('referral_received_date') }} end
    )                                           as dv_financial_year,
    coalesce(
        dv_financial_month_stated,
        case when referral_received_date between '2015-04-01' and current_date()
             then {{ fin_month_from_date('referral_received_date') }} end
    )                                           as dv_financial_month,
    case
        when dv_financial_month_stated is not null and dv_financial_year_stated is not null
            then 'stated'
        when referral_received_date between '2015-04-01' and current_date()
            then 'derived_from_referral_date'
    end                                         as dv_financial_period_source,

    -- DLP standard fields (1-5)
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_financial_month,
    dlp_financial_year, dlp_baseline_financial_month,

    -- Spec body (fields 6-20, in spec order)
    service_request_id, local_patient_id, sk_patient_id, dv_year_of_birth,
    partial_postcode, gender_code, ethnic_category_code, gp_practice_code,
    source_of_referral_code, referral_received_date, team_type_code,
    priority_type_code, primary_referral_reason_code, service_reporting_line,
    provider_code,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
