select
    cluster_id,
    cluster_description,
    code,
    code_description,
    source
from {{ ref('raw_reference_combined_codesets') }}
