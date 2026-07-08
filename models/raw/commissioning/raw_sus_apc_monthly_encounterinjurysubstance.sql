{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterInjurySubstance \ndbt: source(''sus_apc_monthly'', ''EncounterInjurySubstance'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  InjuryAlcoholOrDrugInvolvement_SNOMEDCT -> injury_alcohol_or_drug_involvement_snomedct\n  InjuryAlcoholOrDrugInvolvement_SNOMEDCT -> injury_alcohol_or_drug_involvement_snomedct_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "InjuryAlcoholOrDrugInvolvement_SNOMEDCT" as injury_alcohol_or_drug_involvement_snomedct,
    "InjuryAlcoholOrDrugInvolvement_SNOMEDCT" as injury_alcohol_or_drug_involvement_snomedct_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterInjurySubstance') }}
