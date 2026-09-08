-- Label and person joins must retain each selected source key and attendance code.
select 'referral' as source_entity,
    coalesce(r.unique_service_request_identifier, f.unique_service_request_identifier) as referral_id,
    null::varchar as contact_id,
    iff(r.unique_service_request_identifier is null, 'unexpected_key', 'missing_key') as failure_reason
from {{ ref('stg_csds_cyp101referral') }} as r
full join {{ ref('fct_csds_referral') }} as f using (unique_service_request_identifier)
where r.unique_service_request_identifier is null or f.unique_service_request_identifier is null
union all
select 'contact', coalesce(c.unique_service_request_identifier, f.unique_service_request_identifier),
    coalesce(c.unique_care_contact_identifier, f.unique_care_contact_identifier),
    case when c.unique_care_contact_identifier is null then 'unexpected_key'
        when f.unique_care_contact_identifier is null then 'missing_key'
        else 'attendance_changed' end
from {{ ref('stg_csds_cyp201carecontact') }} as c
full join {{ ref('fct_csds_care_contact') }} as f
    using (unique_service_request_identifier, unique_care_contact_identifier)
where c.unique_care_contact_identifier is null or f.unique_care_contact_identifier is null
    or c.attended_or_did_not_attend_code is distinct from f.attendance_code
