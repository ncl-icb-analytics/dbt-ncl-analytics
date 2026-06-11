select
    sk_organisation_id,
    practice_code as gp_practice_code,
    practice_name,
    practice_name_short,
    borough,
    pcn_code,
    pcn_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_lookup_ncl_gp_practice') }}