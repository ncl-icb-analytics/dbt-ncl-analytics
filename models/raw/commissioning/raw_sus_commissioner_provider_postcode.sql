select
    "COMMISSIONERCODE" as commissioner_code,
    "PROVIDERCODE" as provider_code,
    "EFFECTIVEFROM" as effective_from,
    "EFFECTIVETO" as effective_to
from {{ source('sus_commissioner_reference', 'provider_postcode_commissioner') }}
