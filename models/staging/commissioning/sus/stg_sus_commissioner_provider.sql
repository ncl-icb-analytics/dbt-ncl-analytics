select
    trim(provider_code) as provider_code,
    trim(commissioner_code) as commissioner_code,
    cast(effective_from as date) as effective_from,
    coalesce(cast(effective_to as date), '9999-12-31'::date) as effective_to
from {{ ref('raw_sus_commissioner_provider') }}
where cast(effective_from as date) <= coalesce(cast(effective_to as date), '9999-12-31'::date)
