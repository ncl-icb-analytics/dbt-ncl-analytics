-- Staging model for dictionary_ip.NeoNatalCareLevel
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_NeoNatalCareLevelID" as sk_neo_natal_care_level_id,
    "NeoNatalCareCode" as neo_natal_care_code,
    "ShortCareDescription" as short_care_description,
    "LongCareDescription" as long_care_description
from {{ source('dictionary_ip', 'NeoNatalCareLevel') }}
