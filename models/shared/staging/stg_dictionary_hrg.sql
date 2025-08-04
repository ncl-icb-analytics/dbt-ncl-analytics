-- Staging model for dictionary.HRG
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_HRGID" as sk_hrgid,
    "HRGCode" as hrgcode,
    "HRGDescription" as hrgdescription,
    "HRGChapterKey" as hrgchapterkey,
    "HRGChapter" as hrgchapter,
    "HRGSubchapterKey" as hrgsubchapterkey,
    "HRGSubchapter" as hrgsubchapter,
    "HRG_Version" as hrg_version,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'HRG') }}
