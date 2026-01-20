{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.WorklistRemovalType \ndbt: source(''dictionary_eRS'', ''WorklistRemovalType'') \nColumns:\n  Code -> code\n  Meaning -> meaning\n  Display -> display"
    )
}}
select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display
from {{ source('dictionary_eRS', 'WorklistRemovalType') }}
