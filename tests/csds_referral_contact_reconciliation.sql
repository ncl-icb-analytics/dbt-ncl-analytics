-- Label and person joins must retain the complete source population.
with totals as (
    select 'referral' as source_entity,
        (select count(*) from {{ ref('stg_csds_cyp101referral') }}) as source_count,
        (select count(*) from {{ ref('fct_csds_referral') }}) as fact_count
    union all
    select 'contact',
        (select count(*) from {{ ref('stg_csds_cyp201carecontact') }}),
        (select count(*) from {{ ref('fct_csds_care_contact') }})
    union all
    select 'contact_unknown_attendance',
        (select count(*) from {{ ref('stg_csds_cyp201carecontact') }}
            where attended_or_did_not_attend_code is null),
        (select count(*) from {{ ref('fct_csds_care_contact') }} where attendance_code is null)
)
select source_entity, source_count, fact_count
from totals
where source_count <> fact_count
