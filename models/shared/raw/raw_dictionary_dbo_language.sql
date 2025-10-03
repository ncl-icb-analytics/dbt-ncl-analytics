-- Raw layer model for dictionary_dbo.Language
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LanguageID" as sk_language_id,
    "LanguageSpoken" as language_spoken,
    "CDSCode" as cds_code,
    "Read2Code" as read2_code,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Language') }}
