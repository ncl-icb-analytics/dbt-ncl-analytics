{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterPatient \ndbt: source(''sus_apc_monthly'', ''EncounterPatient'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  SK_GenderID -> sk_gender_id\n  SK_GenderID -> sk_gender_id_1\n  SK_EthnicityID -> sk_ethnicity_id\n  SK_EthnicityID -> sk_ethnicity_id_1\n  SK_PostcodeID -> sk_postcode_id\n  SK_PostcodeID -> sk_postcode_id_1\n  SK_PracticeID -> sk_practice_id\n  SK_PracticeID -> sk_practice_id_1\n  Date_of_Birth -> date_of_birth\n  Date_of_Birth -> date_of_birth_1\n  Age -> age\n  Age -> age_1\n  SK_Org_PracticeID -> sk_org_practice_id\n  SK_Org_PracticeID -> sk_org_practice_id_1\n  WardCode -> ward_code\n  WardCode -> ward_code_1\n  LSOACode -> lsoa_code\n  LSOACode -> lsoa_code_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "SK_GenderID" as sk_gender_id,
    "SK_GenderID" as sk_gender_id_1,
    "SK_EthnicityID" as sk_ethnicity_id,
    "SK_EthnicityID" as sk_ethnicity_id_1,
    "SK_PostcodeID" as sk_postcode_id,
    "SK_PostcodeID" as sk_postcode_id_1,
    "SK_PracticeID" as sk_practice_id,
    "SK_PracticeID" as sk_practice_id_1,
    "Date_of_Birth" as date_of_birth,
    "Date_of_Birth" as date_of_birth_1,
    "Age" as age,
    "Age" as age_1,
    "SK_Org_PracticeID" as sk_org_practice_id,
    "SK_Org_PracticeID" as sk_org_practice_id_1,
    "WardCode" as ward_code,
    "WardCode" as ward_code_1,
    "LSOACode" as lsoa_code,
    "LSOACode" as lsoa_code_1
from {{ source('sus_apc_monthly', 'EncounterPatient') }}
