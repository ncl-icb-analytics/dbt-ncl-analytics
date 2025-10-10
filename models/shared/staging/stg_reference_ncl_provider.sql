select
    sk_organisation_id,
    provider_code,
    provider_name,
    provider_shorthand,
    reporting_code,
    reporting_name,
    reporting_shorthand,
    provider_type,
    row_type
from {{ ref('raw_reference_ncl_provider') }}
