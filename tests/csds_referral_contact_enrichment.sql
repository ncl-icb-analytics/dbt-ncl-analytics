-- A successful reference or parent match must remain visible in the reporting fact.
select 'referral' as source_entity, f.source_record_id
from {{ ref('fct_csds_referral') }} as f
left join {{ ref('organisation') }} as provider
    on upper(trim(f.provider_organisation_code)) = provider.organisation_code
where f.provider_organisation_name is distinct from provider.organisation_name
union all
select 'contact', f.source_record_id
from {{ ref('fct_csds_care_contact') }} as f
left join {{ ref('organisation') }} as provider
    on upper(trim(f.provider_organisation_code)) = provider.organisation_code
left join {{ ref('organisation') }} as site
    on upper(trim(f.site_code)) = site.organisation_code
left join {{ ref('attendance_status') }} as attendance
    on nullif(ltrim(trim(f.source_attendance_status_code), '0'), '') = attendance.code
left join {{ ref('fct_csds_referral') }} as referral
    on f.referral_id = referral.source_record_id
where f.provider_organisation_name is distinct from provider.organisation_name
    or f.site_name is distinct from site.organisation_name
    or f.source_attendance_status_name is distinct from attendance.description
    or f.is_referral_linked is distinct from (referral.source_record_id is not null)
    or f.is_referral_person_consistent is distinct from case
        when referral.source_record_id is null or f.person_id is null or referral.person_id is null then null
        else f.person_id = referral.person_id
    end
