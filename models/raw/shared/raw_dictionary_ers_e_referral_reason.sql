{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.E-Referral_Reason \ndbt: source(''dictionary_eRS'', ''E-Referral_Reason'') \nColumns:\n  Code -> code\n  Meaning -> meaning\n  Display -> display\n  Usage -> usage"
    )
}}
select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display,
    "Usage" as usage
from {{ source('dictionary_eRS', 'E-Referral_Reason') }}
