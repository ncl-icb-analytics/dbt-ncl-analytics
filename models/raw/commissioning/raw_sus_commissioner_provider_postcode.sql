{{
    config(
        description="Raw date-effective provider-postcode-to-commissioner lookup from SUS_COMMISSIONER_PROVIDER_POSTCODE. Columns are renamed to dbt conventions."
    )
}}

select
    "COMMISSIONERCODE" as commissioner_code,
    "PROVIDERCODE" as provider_code,
    "EFFECTIVEFROM" as effective_from,
    "EFFECTIVETO" as effective_to
from {{ source('sus_commissioner_reference', 'provider_postcode_commissioner') }}
