with activity as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , c.person_id
        , b.sk_patient_id
        , c.org_id_prov
        , c.care_contact_date
        , r.ic_age_at_service_referral_received_date
        , c.attendance_status
        , nullif(trim(st.team_type_code), '') as team_type_code
        , nullif(trim(r.primary_reason_for_referral_community_care), '') as primary_referral_reason
    from {{ ref('stg_csds_cyp201carecontact') }} as c
    left join {{ ref('stg_csds_cyp101referral') }} as r
        on c.unique_service_request_identifier = r.unique_service_request_identifier
    left join {{ ref('stg_csds_servicetype') }} as st
        on c.unique_service_request_identifier = st.unique_service_request_identifier
    left join {{ ref('stg_csds_bridging') }} as b
        on c.person_id = b.person_id
)

, categorised as (
    select
        *
        , case
            when ic_age_at_service_referral_received_date is null
                or ic_age_at_service_referral_received_date < 0 then 'Adult'
            when ic_age_at_service_referral_received_date between 0 and 17 then 'CYP'
            else 'Adult'
        end as age_category
        -- lpad guards against zero-padded codes; nulls (~54% of contacts,
        -- structural across providers) costed per NHSE v1.1 guidance
        , lpad(attendance_status, 2, '0') in ('05', '06') or attendance_status is null as is_costed_attendance
    from activity
)

, classified as (
    select
        c.*
        , tt.team_type_name
        , tt.currency_code as team_currency_code
        , tt.is_reason_split
        , rr.currency_code as reason_currency_code
    from categorised as c
    left join {{ ref('nhse_community_currency_team_types_2526') }} as tt
        on c.age_category = tt.age_category
        and c.team_type_code = tt.team_type_code
    left join {{ ref('nhse_community_currency_referral_reasons_2526') }} as rr
        on c.age_category = rr.age_category
        and c.team_type_code = rr.team_type_code
        and c.primary_referral_reason = rr.primary_referral_reason
)

select
    unique_service_request_identifier
    , unique_care_contact_identifier
    , person_id
    , sk_patient_id
    , org_id_prov
    , care_contact_date
    , ic_age_at_service_referral_received_date
    , age_category
    , attendance_status
    , is_costed_attendance
    , team_type_code
    , team_type_name
    , primary_referral_reason
    , case
        when team_currency_code is not null and is_reason_split then coalesce(reason_currency_code, team_currency_code)
        when team_currency_code is not null then team_currency_code
        else iff(age_category = 'CYP', 'CCO99Z', 'CAO99Z')
    end as currency_code
    , case
        when team_currency_code is null then 'unmapped_team'
        when is_reason_split and reason_currency_code is not null then 'reason_split'
        when is_reason_split then 'team_fallback'
        else 'team_type'
    end as currency_source
from classified
