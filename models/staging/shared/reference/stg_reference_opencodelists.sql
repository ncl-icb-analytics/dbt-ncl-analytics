select
    codelist_org,
    codelist_slug,
    codelist_name,
    version_hash,
    coding_system,
    code,
    term,
    cluster_id
from {{ ref('raw_reference_opencodelists') }}
