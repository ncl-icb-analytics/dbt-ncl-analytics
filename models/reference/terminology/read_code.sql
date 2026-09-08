with memberships as (
    select 'read_v2' as coding_system, read_code, read_code_alt, term, snomed_ct_code
    from {{ ref('stg_dictionary_dbo_readcodes') }}
    where is_read_v2
    union all
    select 'ctv3', read_code, read_code_alt, term, snomed_ct_code
    from {{ ref('stg_dictionary_dbo_readcodes') }}
    where is_ctv3
)
, candidates as (
    select coding_system, read_code as code, read_code as dictionary_read_code, term,
        snomed_ct_code, 'exact_code' as match_type, 1 as priority
    from memberships
    union all
    select coding_system, read_code_alt, read_code, term,
        snomed_ct_code, 'dictionary_alternative', 2
    from memberships
    where read_code_alt is not null
)
, preferred as (
    select *
    from candidates
    qualify priority = min(priority) over (partition by coding_system, code)
)
select coding_system, code, dictionary_read_code, term, match_type,
    -- Non-positive dictionary values indicate no usable SNOMED mapping.
    iff(try_to_number(snomed_ct_code) > 0, snomed_ct_code::varchar, null) as snomed_ct_code
from preferred
-- Ambiguous alternatives remain unresolved rather than selecting an arbitrary term.
qualify count(*) over (partition by coding_system, code) = 1
