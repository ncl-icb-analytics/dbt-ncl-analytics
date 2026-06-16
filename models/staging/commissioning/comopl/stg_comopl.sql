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
-- Coded fields are normalised to their national code set where recognisable
-- (gender/priority word maps, zero-padding of leading-zero-stripped numerics,
-- common consultation/cancellation phrasings, free-text ethnicity via the
-- shared nhs_ethnicity_category_code macro proven on stg_mhcorl); genuine
-- provider-local codes pass through unchanged. Validated 2026-06 against the
-- Appendix 5a NationalCodes_* sets: gender/ethnic 100%, priority/team/
-- consultation ~98%, attendance 99%. Source-of-referral (~67%) and cancellation
-- reason (~58%) retain local-code / free-text residue not nationally mappable.
--
-- Financial period (dv_financial_year/month) uses the provider-stated value,
-- falling back to the contact date when absent (the SLAM #806 lesson): lifts
-- coverage ~56% -> ~93%. dv_financial_period_source records which was used.
-- GP practice is cleaned to a valid 6-char ODS code (1 letter + 5 digits).
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

        -- 1-5: DLP standard fields (only ~56% of rows carry them)
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_financial_month                     as dlp_financial_month,
        dlp_financial_year                      as dlp_financial_year,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period (coalesce DLP with plain financial
        -- cols). Activity-date fallback applied in the final select.
        {{ parse_slam_financial_month('coalesce(dlp_financial_month, financial_month)') }}
                                                as dv_financial_month_stated,
        case
            when trim(coalesce(dlp_financial_year, financial_year)) rlike '^20[0-9]{2}$'
                then cast(coalesce(dlp_financial_year, financial_year) as int)
        end                                     as dv_financial_year_stated,

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

        -- 10: Gender -> national code (GENDER carries it; maps M/F/words)
        {{ nc_gender('coalesce(person_stated_gender_code, gender)') }}
                                                as gender_code,
        -- 11: Ethnic category -> NHS national code (maps free-text variants)
        {{ nhs_ethnicity_category_code('coalesce(ethnic_category, patient_ethnicity_code, ethnicity)') }}
                                                as ethnic_category_code,
        -- 12: Registered GP practice (ODS 6-char: 1 letter + 5 digits; strip
        -- branch suffix, drop garbage / '999')
        case
            when coalesce(general_medical_practice_code_patient_registration, gp_practice_code)
                rlike '^[A-Za-z][0-9]{5}'
            then upper(left(coalesce(general_medical_practice_code_patient_registration, gp_practice_code), 6))
        end                                     as gp_practice_code,

        -- 13: Source of referral (sparse here; zero-pad national, local pass through)
        {{ nc_pad('coalesce(source_of_referral_for_community, referral_source)', 2) }}
                                                as source_of_referral_code,
        -- 14: Referral request received date
        {{ parse_uk_date('coalesce(referral_date, dateof_referral, referral_received_date)') }}
                                                as referral_received_date,
        -- 15: Referral request received time
        nullif(trim(referral_request_received_time), '')
                                                as referral_received_time,
        -- 16: Service or team type referred to (zero-pad national 01-56)
        {{ nc_pad('coalesce(service_or_team_type_referred_to_community_care, team_type)', 2) }}
                                                as team_type_code,
        -- 17: Priority type -> national code (1 Routine, 2 Urgent, 3 TWW)
        {{ nc_priority('coalesce(priority_type, priority_code)') }}
                                                as priority_type_code,
        -- 18: Care contact date
        {{ parse_uk_date('coalesce(care_contact_date, contact_date)') }}
                                                as contact_date,
        -- 19: Care contact time
        nullif(trim(care_contact_time), '')     as contact_time,
        -- 20: Care contact cancellation reason (01 patient, 02 provider; map words)
        case
            when upper(trim(care_contact_cancellation_reason)) like '%PATIENT%' then '01'
            when upper(trim(care_contact_cancellation_reason)) like 'CANCELLED BY UNIT%'
              or upper(trim(care_contact_cancellation_reason)) like 'CANCELLED BY SERVICE%'
              or upper(trim(care_contact_cancellation_reason)) like '%NON-CLINICAL%'    then '02'
            else {{ nc_pad('care_contact_cancellation_reason', 2) }}
        end                                     as contact_cancellation_reason,
        -- 21: Consultation type (01 first, 02 follow-up; map words, zero-pad)
        case
            when upper(trim(consultation_type)) like 'FIRST%'                    then '01'
            when upper(trim(consultation_type)) like 'FOLLOW%'                   then '02'
            else {{ nc_pad('consultation_type', 2) }}
        end                                     as consultation_type_code,
        -- 22: Consultation mechanism (zero-pad national 01-98; map common words)
        case
            when upper(trim(consultation_medium_used)) like 'FACE TO FACE%'      then '01'
            when upper(trim(consultation_medium_used)) like 'TELEPHONE%'         then '02'
            else {{ nc_pad('consultation_medium_used', 2) }}
        end                                     as consultation_mechanism_code,
        -- 23: Attendance status (national single-digit 2-7; passthrough trimmed)
        nullif(trim(attendance_status), '')     as attendance_status_code,
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

-- Final projection. Derived reporting period sits up front (after the meta
-- keys); financial period is the stated value, else derived from the contact
-- date (gated to a plausible range so junk dates cannot create phantom
-- periods), with dv_financial_period_source recording which was used.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived)
    coalesce(
        dv_financial_year_stated,
        case when contact_date between '2015-04-01' and current_date()
             then {{ fin_year_start_from_date('contact_date') }} end
    )                                           as dv_financial_year,
    coalesce(
        dv_financial_month_stated,
        case when contact_date between '2015-04-01' and current_date()
             then {{ fin_month_from_date('contact_date') }} end
    )                                           as dv_financial_month,
    case
        when dv_financial_month_stated is not null and dv_financial_year_stated is not null
            then 'stated'
        when contact_date between '2015-04-01' and current_date()
            then 'derived_from_contact_date'
    end                                         as dv_financial_period_source,

    -- DLP standard fields (1-5)
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_financial_month,
    dlp_financial_year, dlp_baseline_financial_month,

    -- Spec body (fields 6-28, in spec order)
    local_patient_id, sk_patient_id, dv_year_of_birth, partial_postcode,
    gender_code, ethnic_category_code, gp_practice_code, source_of_referral_code,
    referral_received_date, referral_received_time, team_type_code,
    priority_type_code, contact_date, contact_time, contact_cancellation_reason,
    consultation_type_code, consultation_mechanism_code, attendance_status_code,
    discharge_date, provider_code, service_reporting_line, service_pod,
    service_request_id,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
