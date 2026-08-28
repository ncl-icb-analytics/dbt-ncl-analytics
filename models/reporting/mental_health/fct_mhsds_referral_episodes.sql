with contact_aggregates as (
    select
        referral_source_record_id
        , count(*) as n_contacts
        , count_if(
            lpad(attendance_status_code, 2, '0') in ('05', '06')
            or attendance_status_code is null
        ) as n_attended_contacts
        , count_if(lpad(attendance_status_code, 2, '0') in ('03', '07')) as n_dna_contacts
        , count_if(lpad(attendance_status_code, 2, '0') in ('02', '04')) as n_cancelled_contacts
        , min(care_contact_date) as first_contact_date
        , min(iff(
            lpad(attendance_status_code, 2, '0') in ('05', '06')
            or attendance_status_code is null
            , care_contact_date
            , null
        )) as first_attended_contact_date
    from {{ ref('fct_mhsds_care_contact') }}
    group by referral_source_record_id
)

, indirect_activity_aggregates as (
    select
        uniq_serv_req_id
        , count(*) as n_indirect_activities
    from {{ ref('stg_mhsds_indirectactivity') }}
    group by uniq_serv_req_id
)

, spell_aggregates as (
    select
        uniq_serv_req_id
        , count(*) as n_spells
        , count(*) > 0 as has_inpatient_spell
    from {{ ref('int_mhsds_spell_currency') }}
    group by uniq_serv_req_id
)

select
    r.source_record_id
    , r.source_dataset
    , r.uniq_serv_req_id
    , r.person_id
    , r.sk_patient_id
    , r.provider_organisation_code
    , r.derived_icb_commissioner_code
    , r.referral_request_received_date
    , r.age_at_referral
    , coalesce(r.age_at_referral < 18, false) as is_cyp_at_referral
    , r.source_of_referral_code
    , r.referring_care_professional_type_code
    , r.clinical_response_priority_code
    , r.primary_reason_for_referral_code
    , rr.population_category as referral_reason_category
    , st.serv_team_type_ref_to_mh as service_or_team_type_code
    , st.service_type_name as source_service_or_team_type_name
    , tt.population_category as team_type_category
    , tt.setting_group
    , tt.setting_name
    -- This duplicates the contact model's crisis rule pending a shared home.
    , coalesce(
        coalesce(tt.is_crisis_referral, false)
        or (
            coalesce(tt.crisis_requires_urgent_priority, false)
            and r.clinical_response_priority_code in ('1', '2', '4')
        )
        , false
    ) as is_crisis_referral
    , coalesce(c.n_contacts, 0) as n_contacts
    , coalesce(c.n_attended_contacts, 0) as n_attended_contacts
    , coalesce(c.n_dna_contacts, 0) as n_dna_contacts
    , coalesce(c.n_cancelled_contacts, 0) as n_cancelled_contacts
    , c.first_contact_date
    , c.first_attended_contact_date
    , iff(
        c.first_attended_contact_date < r.referral_request_received_date
        , null
        , datediff(
            day
            , r.referral_request_received_date
            , c.first_attended_contact_date
        )
    ) as wait_days_to_first_attended_contact
    , coalesce(
        c.first_attended_contact_date < r.referral_request_received_date
        , false
    ) as has_pre_referral_contact
    , coalesce(i.n_indirect_activities, 0) as n_indirect_activities
    , coalesce(s.n_spells, 0) as n_spells
    , coalesce(s.has_inpatient_spell, false) as has_inpatient_spell
    , r.referral_rejection_date
    , r.rejection_reason_code
    , r.referral_status = 'rejected' as is_rejected
    , r.referral_discharge_date
    , r.closure_reason_code
    , r.referral_status
from {{ ref('fct_mhsds_referral') }} as r
left join {{ ref('nhse_mh_currency_referral_reasons_2627') }} as rr
    on r.primary_reason_for_referral_code = rr.prim_reason_referral_mh
left join {{ ref('stg_mhsds_servicetype') }} as st
    on r.uniq_serv_req_id = st.uniq_serv_req_id
left join {{ ref('nhse_mh_currency_team_types_2627') }} as tt
    on st.serv_team_type_ref_to_mh = tt.serv_team_type
left join contact_aggregates as c
    on r.source_record_id = c.referral_source_record_id
left join indirect_activity_aggregates as i
    on r.uniq_serv_req_id = i.uniq_serv_req_id
left join spell_aggregates as s
    on r.uniq_serv_req_id = s.uniq_serv_req_id
