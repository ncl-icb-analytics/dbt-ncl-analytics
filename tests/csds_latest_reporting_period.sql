-- A late refresh of an older month must not replace a newer reported state.
with referral_latest as (
    select unique_service_request_identifier, max(reporting_period_end_date) as latest_period
    from {{ ref('stg_csds_referral_history') }}
    group by unique_service_request_identifier
), contact_latest as (
    select unique_service_request_identifier, unique_care_contact_identifier,
        max(reporting_period_end_date) as latest_period
    from {{ ref('stg_csds_care_contact_history') }}
    group by unique_service_request_identifier, unique_care_contact_identifier
)
select 'referral' as source_entity, count(*) as invalid_count
from {{ ref('stg_csds_cyp101referral') }} as r
join referral_latest as h using (unique_service_request_identifier)
where r.reporting_period_end_date is distinct from h.latest_period
having count(*) > 0
union all
select 'contact', count(*)
from {{ ref('stg_csds_cyp201carecontact') }} as c
join contact_latest as h using (unique_service_request_identifier, unique_care_contact_identifier)
where c.reporting_period_end_date is distinct from h.latest_period
having count(*) > 0
