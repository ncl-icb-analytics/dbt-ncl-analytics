select
    trim(organisation_code_practice) as practice_code,
    trim(organisation_code_commissioner) as commissioner_code,
    cast(relationship_start_date as date) as effective_from,
    coalesce(cast(relationship_end_date as date), '9999-12-31'::date) as effective_to,
    cast(is_active as boolean) as is_active,
    cast(is_proxy as boolean) as is_proxy
from {{ ref('raw_sus_commissioner_practice') }}
where cast(relationship_start_date as date) <= coalesce(cast(relationship_end_date as date), '9999-12-31'::date)
