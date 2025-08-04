-- Staging model for dictionary.Language
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_LanguageID" as sk_languageid,
    "LanguageSpoken" as languagespoken,
    "CDSCode" as cdscode,
    "Read2Code" as read2code,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Language') }}
