{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Gender \ndbt: source(''dictionary_dbo'', ''Gender'') \nColumns:\n  SK_GenderID -> sk_gender_id\n  Gender -> gender\n  GenderCode -> gender_code\n  GenderCode1 -> gender_code1\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  GenderCode2 -> gender_code2"
    )
}}
select
    "SK_GenderID" as sk_gender_id,
    "Gender" as gender,
    "GenderCode" as gender_code,
    "GenderCode1" as gender_code1,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "GenderCode2" as gender_code2
from {{ source('dictionary_dbo', 'Gender') }}
