select
    "OACODE" as oa_code,
    "SK_OUTPUTAREAID" as sk_output_area_id,
    "ORGANISATIONCODE_COMMISSIONER" as organisation_code_commissioner,
    "SK_ORGANISATIONID" as sk_organisation_id,
    "EFFECTIVEFROM" as effective_from,
    "EFFECTIVETO" as effective_to
from {{ source('sus_commissioner_reference', 'lsoa_commissioner') }}
