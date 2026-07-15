select
    "PROVIDERCODE" as provider_code,
    "COMMISSIONERCODE" as commissioner_code,
    "EFFECTIVEFROM" as effective_from,
    "EFFECTIVETO" as effective_to
from {{ source('sus_commissioner_reference', 'provider_commissioner') }}
