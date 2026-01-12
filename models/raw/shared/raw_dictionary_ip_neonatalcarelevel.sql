{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.NeoNatalCareLevel \ndbt: source(''dictionary_ip'', ''NeoNatalCareLevel'') \nColumns:\n  SK_NeoNatalCareLevelID -> sk_neo_natal_care_level_id\n  NeoNatalCareCode -> neo_natal_care_code\n  ShortCareDescription -> short_care_description\n  LongCareDescription -> long_care_description"
    )
}}
select
    "SK_NeoNatalCareLevelID" as sk_neo_natal_care_level_id,
    "NeoNatalCareCode" as neo_natal_care_code,
    "ShortCareDescription" as short_care_description,
    "LongCareDescription" as long_care_description
from {{ source('dictionary_ip', 'NeoNatalCareLevel') }}
