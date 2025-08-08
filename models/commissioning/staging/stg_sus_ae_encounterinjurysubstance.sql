-- Staging model for sus_ae.EncounterInjurySubstance
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "InjuryAlcoholOrDrugInvolvement_SNOMEDCT" as injuryalcoholordruginvolvement_snomedct,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterInjurySubstance') }}
