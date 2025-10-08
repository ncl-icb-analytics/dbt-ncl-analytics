select
    cluster_id,
    cluster_description,
    code,
    code_description,
    source
from {{ ref('raw_reference_combined_codesets') }}
qualify row_number() over (partition by cluster_id, code, source order by code_description) = 1
