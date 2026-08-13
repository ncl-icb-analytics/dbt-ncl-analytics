-- Current-state fields use the active MHSDS feed, which is about six weeks
-- behind the run date. Contact windows therefore have the same lag.
with population as (
    select person_id
    from {{ ref('stg_mhsds_referral') }}
    where person_id is not null

    union

    select person_id
    from {{ ref('stg_mhsds_carecontact') }}
    where person_id is not null

    union

    select person_id
    from {{ ref('int_mhsds_spell_currency') }}
    where person_id is not null
)

, referral_aggregates as (
    select
        person_id
        , count(*) as n_referrals_ever
        , count_if(episode_status = 'open') as n_open_referrals
        , min(referral_request_received_date) as first_referral_date
        , max(referral_request_received_date) as latest_referral_date
    from {{ ref('fct_mhsds_referral_episodes') }}
    where person_id is not null
    group by person_id
)

, contact_aggregates as (
    select
        person_id
        , max(care_cont_date) as latest_contact_date
        , count_if(care_cont_date >= dateadd(month, -12, current_date)) > 0
            as has_contact_last_12m
        , count_if(care_cont_date >= dateadd(day, -90, current_date)) > 0
            as has_contact_last_90d
        , count_if(
            care_cont_date >= dateadd(month, -12, current_date)
            and (
                lpad(attend_status, 2, '0') in ('05', '06')
                or attend_status is null
            )
        ) as n_attended_contacts_12m
    from {{ ref('stg_mhsds_carecontact') }}
    where person_id is not null
    group by person_id
)

, crisis_contact_aggregates as (
    select
        c.person_id
        , count_if(
            c.care_cont_date >= dateadd(month, -12, current_date)
            and (
                lpad(c.attend_status, 2, '0') in ('05', '06')
                or c.attend_status is null
            )
        ) > 0 as has_crisis_contact_12m
    from {{ ref('stg_mhsds_carecontact') }} as c
    inner join {{ ref('fct_mhsds_referral_episodes') }} as r
        on c.uniq_serv_req_id = r.uniq_serv_req_id
    where c.person_id is not null
        and r.is_crisis_referral
    group by c.person_id
)

, current_inpatient_aggregates as (
    select
        person_id
        , true as is_current_inpatient
    from {{ ref('fct_mhsds_current_inpatients') }}
    where person_id is not null
    group by person_id
)

, spell_aggregates as (
    select
        person_id
        , max(start_date_hosp_prov_spell) as latest_admission_date
        , max(iff(end_date_source = 'discharged', end_date, null))
            as latest_discharge_date
        , count(*) as n_spells_ever
        , count(*) > 0 as ever_inpatient
    from {{ ref('int_mhsds_spell_currency') }}
    where person_id is not null
    group by person_id
)

, latest_diagnosis as (
    select
        r.person_id
        , d.icd10_3
        , g.population_category as diagnosis_category
        , d.coded_diag_timestamp as latest_diagnosis_date
    from {{ ref('stg_mhsds_primdiag') }} as d
    inner join {{ ref('fct_mhsds_referral_episodes') }} as r
        on d.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_icd10_groups_2627') }} as g
        on d.icd10_3 between g.icd10_range_start and g.icd10_range_end
    qualify row_number() over (
        partition by r.person_id
        order by d.coded_diag_timestamp desc, d.icd10_3
    ) = 1
)

, mha_aggregates as (
    select
        person_id
        , count_if(
            nhsd_legal_status is not null
            and nhsd_legal_status not in ('01', '98', '99')
        ) > 0 as ever_detained
        , max(iff(
            nhsd_legal_status is not null
            and nhsd_legal_status not in ('01', '98', '99')
            , start_date_mh_act_legal_status_class
            , null
        )) as latest_detention_start_date
        -- REVIEW: expiry dates are treated as current through the expiry date;
        -- confirm this matches MHSDS legal-status expiry semantics.
        , count_if(
            nhsd_legal_status is not null
            and nhsd_legal_status not in ('01', '98', '99')
            and end_date_mh_act_legal_status_class is null
            and (
                expiry_date_mh_act_legal_status_class is null
                or expiry_date_mh_act_legal_status_class >= current_date
            )
        ) > 0 as is_currently_detained
    from {{ ref('stg_mhsds_mhactperiod') }}
    where person_id is not null
    group by person_id
)

, latest_patient_indicators as (
    select
        person_id
        , cpp
        , lac_status
    from {{ ref('stg_mhsds_patientindicators') }}
    where person_id is not null
    qualify row_number() over (
        partition by person_id
        order by reporting_period_end_date desc nulls last, mhs005_uniq_id
    ) = 1
)

, safeguarding_aggregates as (
    -- CPP code semantics are unverified against the MHSDS TOS ('1' covers
    -- ~40% of rows, implausible for active plans) - the raw code is exposed
    -- rather than a guessed boolean
    select
        person_id
        , upper(cpp) as cpp_status_code
        , upper(lac_status) = 'Y' as is_looked_after_child
    from latest_patient_indicators
)

select
    p.person_id
    , b.sk_patient_id
    , coalesce(r.n_referrals_ever, 0) as n_referrals_ever
    , coalesce(r.n_open_referrals, 0) as n_open_referrals
    , r.first_referral_date
    , r.latest_referral_date
    , c.latest_contact_date
    , coalesce(c.has_contact_last_12m, false) as has_contact_last_12m
    , coalesce(c.has_contact_last_90d, false) as has_contact_last_90d
    , coalesce(c.n_attended_contacts_12m, 0) as n_attended_contacts_12m
    , coalesce(cc.has_crisis_contact_12m, false) as has_crisis_contact_12m
    , coalesce(ci.is_current_inpatient, false) as is_current_inpatient
    , s.latest_admission_date
    , s.latest_discharge_date
    , coalesce(s.n_spells_ever, 0) as n_spells_ever
    , coalesce(s.ever_inpatient, false) as ever_inpatient
    , d.icd10_3
    , d.diagnosis_category
    , d.latest_diagnosis_date
    , coalesce(m.ever_detained, false) as ever_detained
    , m.latest_detention_start_date
    , coalesce(m.is_currently_detained, false) as is_currently_detained
    , g.cpp_status_code
    , coalesce(g.is_looked_after_child, false) as is_looked_after_child
from population as p
left join {{ ref('stg_mhsds_bridging') }} as b
    on p.person_id = b.person_id
left join referral_aggregates as r
    on p.person_id = r.person_id
left join contact_aggregates as c
    on p.person_id = c.person_id
left join crisis_contact_aggregates as cc
    on p.person_id = cc.person_id
left join current_inpatient_aggregates as ci
    on p.person_id = ci.person_id
left join spell_aggregates as s
    on p.person_id = s.person_id
left join latest_diagnosis as d
    on p.person_id = d.person_id
left join mha_aggregates as m
    on p.person_id = m.person_id
left join safeguarding_aggregates as g
    on p.person_id = g.person_id
