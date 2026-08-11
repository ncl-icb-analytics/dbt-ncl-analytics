with contact_aggregates as (
    select
        uniq_serv_req_id
        , count(*) as n_contacts
        , count_if(
            lpad(attend_status, 2, '0') in ('05', '06')
            or attend_status is null
        ) as n_attended_contacts
        , count_if(lpad(attend_status, 2, '0') in ('03', '07')) as n_dna_contacts
        , count_if(lpad(attend_status, 2, '0') in ('02', '04')) as n_cancelled_contacts
        , min(care_cont_date) as first_contact_date
        , min(iff(
            lpad(attend_status, 2, '0') in ('05', '06')
            or attend_status is null
            , care_cont_date
            , null
        )) as first_attended_contact_date
    from {{ ref('stg_mhsds_carecontact') }}
    group by uniq_serv_req_id
)

, indirect_activities as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs204indirectactivity'),
            partition_cols = ['mhs204_uniq_id']
        )
    }}
)

, indirect_activity_aggregates as (
    select
        uniq_serv_req_id
        , count(*) as n_indirect_activities
    from indirect_activities
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
    r.uniq_serv_req_id
    , r.person_id
    , b.sk_patient_id
    , r.org_id_prov
    , r.dm_icb_commissioner
    , r.referral_request_received_date
    , r.age_serv_refer_rec_date
    , coalesce(r.age_serv_refer_rec_date < 18, false) as is_cyp_at_referral
    , r.source_of_referral_mh
    , r.referring_care_professional_type
    , r.clin_resp_priority_type
    , r.prim_reason_referral_mh
    , rr.population_category as referral_reason_category
    , st.serv_team_type_ref_to_mh
    , st.service_type_name as team_type_name
    , tt.population_category as team_type_category
    , tt.setting_group
    , tt.setting_name
    -- This duplicates the contact model's crisis rule pending a shared home.
    , coalesce(
        coalesce(tt.is_crisis_referral, false)
        or (
            coalesce(tt.crisis_requires_urgent_priority, false)
            and r.clin_resp_priority_type in ('1', '2', '4')
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
    , r.refer_rejection_date
    , r.refer_reject_reason
    , r.refer_rejection_date is not null as is_rejected
    , r.serv_disch_date
    , r.refer_clos_reason
    , case
        when r.refer_rejection_date is not null then 'rejected'
        when r.serv_disch_date is not null then 'closed'
        else 'open'
    end as episode_status
from {{ ref('stg_mhsds_referral') }} as r
left join {{ ref('stg_mhsds_bridging') }} as b
    on r.person_id = b.person_id
left join {{ ref('nhse_mh_currency_referral_reasons_2627') }} as rr
    on r.prim_reason_referral_mh = rr.prim_reason_referral_mh
left join {{ ref('stg_mhsds_servicetype') }} as st
    on r.uniq_serv_req_id = st.uniq_serv_req_id
left join {{ ref('nhse_mh_currency_team_types_2627') }} as tt
    on st.serv_team_type_ref_to_mh = tt.serv_team_type
left join contact_aggregates as c
    on r.uniq_serv_req_id = c.uniq_serv_req_id
left join indirect_activity_aggregates as i
    on r.uniq_serv_req_id = i.uniq_serv_req_id
left join spell_aggregates as s
    on r.uniq_serv_req_id = s.uniq_serv_req_id
