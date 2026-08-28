{{ config(materialized='table', tags=['mhsds']) }}

with active_contacts as (
    select c.*
    from {{ ref('raw_mhsds_mhs201carecontact') }} as c
    inner join {{ ref('stg_mhsds_activesubmission') }} as a
        on c.uniq_submission_id = a.uniq_submission_id
    qualify row_number() over (
        partition by c.uniq_serv_req_id, c.uniq_care_cont_id
        order by
            c.effective_from desc
            , c.reporting_period_end_date desc
            , c.uniq_submission_id desc
            , c.mhs201_uniq_id desc
    ) = 1
)

select
    uniq_care_cont_id
    , uniq_serv_req_id
    , mhs201_uniq_id
    , care_contact_id
    , service_request_id
    , person_id
    , care_cont_date
    , care_cont_time
    , org_id_prov
    , org_id_comm
    , attend_status
    , attend_or_dna_code
    , clin_cont_dur_of_care_cont
    , care_prof_team_local_id
    , other_care_prof_team_local_id
    , uniq_care_prof_team_id
    , uniq_other_care_prof_team_local_id
    , site_id_of_treat
    , admin_cat_code
    , specialised_mh_service_code
    , cons_type
    , care_cont_subj
    , cons_mechanism_mh
    , cons_medium_used
    , act_loc_type_code
    , place_of_safety_ind
    , group_therapy_ind
    , language_code_treat
    , interpreter_present_ind
    , planned_care_cont_indicator
    , care_cont_patient_ther_mode
    , earliest_reason_offer_date
    , earliest_clin_app_date
    , care_cont_cancel_date
    , care_cont_cancel_reas
    , rep_appt_offer_date
    , rep_appt_book_date
    , reasonable_adjustment_made
    , age_care_cont_date
    , cont_loc_distance_home
    , time_refer_and_care_contact
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dm_commissioner_derivation_reason
    , uniq_submission_id
    , uniq_month_id
    , reporting_period_start_date
    , reporting_period_end_date
    , dmic_dataset
    , effective_from
    , dmic_date_added
from active_contacts
