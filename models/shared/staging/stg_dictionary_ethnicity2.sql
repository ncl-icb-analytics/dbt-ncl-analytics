-- Staging model for dictionary.Ethnicity2
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_EthnicityID" as sk_ethnicityid,
    "EthnicityCategory" as ethnicitycategory,
    "EthnicityDesc" as ethnicitydesc
from {{ source('dictionary', 'Ethnicity2') }}
