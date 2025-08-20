-- Staging model for dictionary_ip.NeoNatalCareLevel
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_NeoNatalCareLevelID" as sk_neonatalcarelevelid,
    "NeoNatalCareCode" as neonatalcarecode,
    "ShortCareDescription" as shortcaredescription,
    "LongCareDescription" as longcaredescription
from {{ source('dictionary_ip', 'NeoNatalCareLevel') }}
