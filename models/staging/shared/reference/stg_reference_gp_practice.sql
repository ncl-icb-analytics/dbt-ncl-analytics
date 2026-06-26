-- WNL GP practice reference (NCL + NWL, 513 practices). Same shape as the
-- legacy NCL-only stg_reference_lookup_ncl_gp_practice; used by the person
-- spine so it covers the whole WNL footprint. NCL-only consumers keep using
-- the legacy model.
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
from {{ ref('raw_reference_lookup_ncl_gp_practice_wnl') }}
