-- Staging model for sus_ae.EncounterPatient
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "SK_GenderID" as sk_genderid,
    "SK_EthnicityID" as sk_ethnicityid,
    "SK_PostcodeID" as sk_postcodeid,
    "SK_PracticeID" as sk_practiceid,
    "Date_of_Birth" as date_of_birth,
    "Age" as age,
    "SK_Org_PracticeID" as sk_org_practiceid,
    "WardCode" as wardcode,
    "LSOACode" as lsoacode
from {{ source('sus_ae', 'EncounterPatient') }}
