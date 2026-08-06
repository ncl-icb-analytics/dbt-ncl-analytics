{{
    config(
        description="Raw date-effective provider-to-commissioner lookup from SUS_COMMISSIONER_PROVIDER. Columns are renamed to dbt conventions."
    )
}}

select
    "PROVIDERCODE" as provider_code,
    "COMMISSIONERCODE" as commissioner_code,
    "EFFECTIVEFROM" as effective_from,
    "EFFECTIVETO" as effective_to
from {{ source('sus_commissioner_reference', 'provider_commissioner') }}
