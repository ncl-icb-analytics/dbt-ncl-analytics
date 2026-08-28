select
    r.uniq_serv_req_id as source_record_id
    , 'MHSDS' as source_dataset
    , r.uniq_serv_req_id
    , r.mhs101_uniq_id
    , r.service_request_id
    , r.person_id
    , b.sk_patient_id
    , r.referral_request_received_date
    , r.referral_request_received_time
    , case
        when r.referral_request_received_time is not null
            then timestamp_ntz_from_parts(
                r.referral_request_received_date
                , r.referral_request_received_time
            )
    end as referral_received_at
    , iff(r.referral_request_received_time is null, 'date', 'timestamp')
        as referral_received_time_precision
    , r.decision_to_treat_date
    , r.decision_to_treat_time
    , case
        when r.decision_to_treat_date is not null
            and r.decision_to_treat_time is not null
            then timestamp_ntz_from_parts(r.decision_to_treat_date, r.decision_to_treat_time)
    end as decision_to_treat_at
    , r.refer_rejection_date
    , r.refer_rejection_time
    , case
        when r.refer_rejection_date is not null
            and r.refer_rejection_time is not null
            then timestamp_ntz_from_parts(r.refer_rejection_date, r.refer_rejection_time)
    end as referral_rejected_at
    , r.serv_disch_date as referral_discharge_date
    , r.serv_disch_time as referral_discharge_time
    , case
        when r.serv_disch_date is not null and r.serv_disch_time is not null
            then timestamp_ntz_from_parts(r.serv_disch_date, r.serv_disch_time)
    end as referral_discharged_at
    , case
        when r.refer_rejection_date is not null then 'rejected'
        when r.serv_disch_date is not null then 'closed'
        else 'open'
    end as referral_status
    , r.source_of_referral_mh as source_of_referral_code
    , sr.description as source_of_referral_description
    , r.clin_resp_priority_type as clinical_response_priority_code
    , r.prim_reason_referral_mh as primary_reason_for_referral_code
    , r.referring_care_professional_type
    , r.referring_care_professional_staff_group
    , r.refer_reject_reason as rejection_reason_code
    , r.refer_clos_reason as closure_reason_code
    , r.reason_oat as out_of_area_treatment_reason_code
    , r.org_id_prov as provider_organisation_code
    , r.org_id_comm as commissioner_organisation_code
    , r.dm_icb_commissioner as derived_icb_commissioner_code
    , r.dm_sub_icb_commissioner as derived_sub_icb_commissioner_code
    , r.dm_commissioner_derivation_reason
    , r.org_id_referring_org as referring_organisation_code
    , r.org_id_referring as referring_organisation_or_person_code
    , r.specialised_mh_service_code
    , r.care_prof_team_local_id as primary_service_or_team_local_id
    , r.uniq_care_prof_team_local_id as primary_service_or_team_id
    , coalesce(r.serv_team_type, td.serv_team_type_mh) as primary_service_or_team_type_code
    , tt.description as primary_service_or_team_type_description
    , coalesce(r.service_type_name, td.service_type_name) as source_service_or_team_type_name
    , coalesce(r.serv_team_int_age_group, td.serv_team_int_age_group)
        as primary_service_or_team_intended_age_group_code
    , r.age_serv_refer_rec_date as age_at_referral
    , r.age_serv_refer_disch_date as age_at_discharge
    , r.disch_letter_iss_date as discharge_letter_issued_date
    , r.record_start_date
    , r.record_end_date
    , r.uniq_submission_id
    , r.uniq_month_id
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.dmic_dataset as mhsds_version
    , r.effective_from as source_effective_from_at
    , r.dmic_date_added as source_loaded_at
from {{ ref('stg_mhsds_referral') }} as r
left join {{ ref('stg_mhsds_bridging') }} as b
    on r.person_id = b.person_id
left join {{ ref('mhsds_source_of_referral') }} as sr
    on r.source_of_referral_mh = sr.code
left join {{ ref('stg_mhsds_service_or_team_details') }} as td
    on r.care_prof_team_local_id = td.care_prof_team_local_id
    and r.uniq_submission_id = td.uniq_submission_id
left join {{ ref('mhsds_service_or_team_type') }} as tt
    on coalesce(r.serv_team_type, td.serv_team_type_mh) = tt.code
