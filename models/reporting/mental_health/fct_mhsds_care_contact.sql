select
    {{ dbt_utils.generate_surrogate_key([
        'c.uniq_serv_req_id',
        'c.uniq_care_cont_id'
    ]) }} as source_record_id
    , 'MHSDS' as source_dataset
    , c.mhs201_uniq_id
    , c.uniq_care_cont_id
    , c.care_contact_id
    , c.uniq_serv_req_id as referral_source_record_id
    , c.service_request_id
    , c.person_id
    , b.sk_patient_id
    , c.care_cont_date as care_contact_date
    , c.care_cont_time as care_contact_time
    , case
        when c.care_cont_time is not null
            then timestamp_ntz_from_parts(c.care_cont_date, c.care_cont_time)
    end as care_contact_at
    , iff(c.care_cont_time is null, 'date', 'timestamp') as care_contact_time_precision
    , c.clin_cont_dur_of_care_cont as clinical_contact_duration_minutes
    , c.attend_status as attendance_status_code
    , ats.description as attendance_status_description
    , c.cons_type as consultation_type_code
    , c.cons_mechanism_mh as consultation_mechanism_code
    , cm.description as consultation_mechanism_description
    , c.act_loc_type_code as activity_location_type_code
    , alt.description as activity_location_type_description
    , c.care_cont_subj as care_contact_subject_code
    , c.planned_care_cont_indicator as planned_care_contact_indicator
    , c.place_of_safety_ind as place_of_safety_indicator
    , c.group_therapy_ind as group_therapy_indicator
    , c.care_cont_patient_ther_mode as patient_therapy_mode_code
    , c.org_id_prov as provider_organisation_code
    , c.org_id_comm as commissioner_organisation_code
    , c.dm_icb_commissioner as derived_icb_commissioner_code
    , c.dm_sub_icb_commissioner as derived_sub_icb_commissioner_code
    , c.dm_commissioner_derivation_reason
    , c.site_id_of_treat as treatment_site_code
    , c.admin_cat_code as administrative_category_code
    , c.specialised_mh_service_code
    , coalesce(
        c.uniq_other_care_prof_team_local_id
        , c.uniq_care_prof_team_id
    ) as service_or_team_id
    , coalesce(
        c.other_care_prof_team_local_id
        , c.care_prof_team_local_id
    ) as service_or_team_local_id
    , td.serv_team_type_mh as service_or_team_type_code
    , tt.description as service_or_team_type_description
    , td.service_type_name as source_service_or_team_type_name
    , td.serv_team_int_age_group as service_or_team_intended_age_group_code
    , c.earliest_reason_offer_date as earliest_reasonable_offer_date
    , c.earliest_clin_app_date as earliest_clinically_appropriate_date
    , c.rep_appt_offer_date as appointment_offer_date
    , c.rep_appt_book_date as appointment_booked_date
    , c.care_cont_cancel_date as care_contact_cancelled_date
    , c.care_cont_cancel_reas as care_contact_cancellation_reason_code
    , c.language_code_treat as treatment_language_code
    , c.interpreter_present_ind as interpreter_present_indicator
    , c.reasonable_adjustment_made as reasonable_adjustment_made_code
    , c.age_care_cont_date as age_at_care_contact
    , c.cont_loc_distance_home as distance_from_home
    , c.time_refer_and_care_contact as minutes_from_referral_to_care_contact
    , c.uniq_submission_id
    , c.uniq_month_id
    , c.reporting_period_start_date
    , c.reporting_period_end_date
    , c.dmic_dataset as mhsds_version
    , c.effective_from as source_effective_from_at
    , c.dmic_date_added as source_loaded_at
from {{ ref('stg_mhsds_carecontact') }} as c
left join {{ ref('stg_mhsds_bridging') }} as b
    on c.person_id = b.person_id
left join {{ ref('stg_mhsds_service_or_team_details') }} as td
    on coalesce(c.other_care_prof_team_local_id, c.care_prof_team_local_id)
        = td.care_prof_team_local_id
    and c.uniq_submission_id = td.uniq_submission_id
left join {{ ref('attendance_status') }} as ats
    on c.attend_status = ats.code
left join {{ ref('consultation_mechanism') }} as cm
    on c.cons_mechanism_mh = cm.code
left join {{ ref('activity_location_type') }} as alt
    on c.act_loc_type_code = alt.code
left join {{ ref('mhsds_service_or_team_type') }} as tt
    on td.serv_team_type_mh = tt.code
