{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Language \ndbt: source(''dictionary_dbo'', ''Language'') \nColumns:\n  SK_LanguageID -> sk_language_id\n  LanguageSpoken -> language_spoken\n  CDSCode -> cds_code\n  Read2Code -> read2_code\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_LanguageID" as sk_language_id,
    "LanguageSpoken" as language_spoken,
    "CDSCode" as cds_code,
    "Read2Code" as read2_code,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Language') }}
