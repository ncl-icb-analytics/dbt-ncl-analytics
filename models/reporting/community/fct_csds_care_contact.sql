select
    {{ dbt_utils.generate_surrogate_key(['r.unique_service_request_identifier', 'r.unique_care_contact_identifier']) }} as source_record_id
    , 'CSDS' as source_dataset
    , r.unique_service_request_identifier as referral_source_record_id
    , r.unique_service_request_identifier
    , r.unique_care_contact_identifier
    , r.care_contact_identifier
    , r.cyp201_unique_id as source_row_id
    , r.person_id
    , b.sk_patient_id
    , r.ic_age_at_care_contact_date as age_at_contact
    , r.care_contact_date::date as care_contact_date
    , r.care_contact_time::time as care_contact_time
    , case
        when r.care_contact_date is null then null
        when r.care_contact_time is null then r.care_contact_date::date::timestamp_ntz
        else timestamp_ntz_from_parts(r.care_contact_date::date, r.care_contact_time::time)
    end as care_contact_at
    , case
        when r.care_contact_date is null then null
        when r.care_contact_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , r.attended_or_did_not_attend_code as attendance_code
    , att.description as attendance_name
    , r.attendance_status as source_attendance_status_code
    , r.consultation_mechanism_community_care as consultation_mechanism_code
    , cm.description as consultation_mechanism_name
    , r.consultation_medium_used as legacy_consultation_medium_code
    , r.activity_location_type_code
    , loc.description as activity_location_type_name
    , r.clinical_contact_duration_of_care_contact as clinical_contact_duration_minutes
    , r.site_code_of_treatment as site_code
    , r.unique_care_professional_team_local_identifier
    , r.care_professional_team_local_identifier
    , r.administrative_category_code
    , r.consultation_type as consultation_type_code
    , r.care_contact_subject as care_contact_subject_code
    , r.group_therapy_indicator
    , r.care_contact_cancellation_date::date as cancellation_date
    , r.care_contact_cancellation_reason as cancellation_reason_code
    , r.earliest_reasonable_offer_date::date as earliest_reasonable_offer_date
    , r.earliest_clinically_appropriate_date::date as earliest_clinically_appropriate_date
    , r.replacement_appointment_date_offered::date as replacement_appointment_date_offered
    , r.replacement_appointment_booked_date::date as replacement_appointment_booked_date
    , p.source_record_id is not null as is_referral_linked
    , case
        when p.source_record_id is null or r.person_id is null or p.person_id is null then null
        else r.person_id = p.person_id
    end as is_referral_person_consistent
    , r.organisation_code_provider as provider_organisation_code
    , r.organisation_code_code_of_commissioner as submitted_commissioner_code
    , r.dm_icb_commissioner as source_icb_commissioner_code
    , r.dm_sub_icb_commissioner as source_sub_icb_commissioner_code
    , r.dm_commissioner_derivation_reason as source_commissioner_derivation_reason
    , r.unique_submission_id
    , r.reporting_period_start_date::date as reporting_period_start_date
    , r.reporting_period_end_date::date as reporting_period_end_date
    , r.effective_from as source_file_received_at
    , r.csds_version
    , r.file_type
    , admin.description as administrative_category_name
    , consult.description as consultation_type_name
    , subject.description as care_contact_subject_name
    , cancel.description as cancellation_reason_name
    , medium.description as legacy_consultation_medium_name
    , therapy.description as group_therapy_indicator_name
from {{ ref('stg_csds_cyp201carecontact') }} as r
left join {{ ref('stg_csds_bridging') }} as b on r.person_id = b.person_id
left join {{ ref('fct_csds_referral') }} as p on r.unique_service_request_identifier = p.source_record_id
left join {{ ref('attendance_status') }} as att on nullif(ltrim(trim(r.attended_or_did_not_attend_code), '0'), '') = att.code
left join {{ ref('consultation_mechanism') }} as cm on trim(r.consultation_mechanism_community_care) = cm.code
left join {{ ref('activity_location_type') }} as loc on trim(r.activity_location_type_code) = loc.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as admin
    on admin.code_set_name = 'administrative_category' and trim(r.administrative_category_code) = admin.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as consult
    on consult.code_set_name = 'consultation_type' and trim(r.consultation_type) = consult.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as subject
    on subject.code_set_name = 'care_contact_subject' and trim(r.care_contact_subject) = subject.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as cancel
    on cancel.code_set_name = 'care_contact_cancellation_reason' and trim(r.care_contact_cancellation_reason) = cancel.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as medium
    on medium.code_set_name = 'consultation_medium_used' and trim(r.consultation_medium_used) = medium.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as therapy
    on therapy.code_set_name = 'group_therapy_indicator' and trim(r.group_therapy_indicator) = therapy.code
