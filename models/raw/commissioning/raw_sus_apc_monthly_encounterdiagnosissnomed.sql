{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterDiagnosisSNOMED \ndbt: source(''sus_apc_monthly'', ''EncounterDiagnosisSNOMED'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  Code -> code\n  Code -> code_1\n  Qualifier -> qualifier\n  Qualifier -> qualifier_1\n  Qualifier_Is_Approved -> qualifier_is_approved\n  Qualifier_Is_Approved -> qualifier_is_approved_1\n  Coded_Clinical_Entry_Sequence_Number -> coded_clinical_entry_sequence_number\n  Coded_Clinical_Entry_Sequence_Number -> coded_clinical_entry_sequence_number_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "Code" as code,
    "Code" as code_1,
    "Qualifier" as qualifier,
    "Qualifier" as qualifier_1,
    "Qualifier_Is_Approved" as qualifier_is_approved,
    "Qualifier_Is_Approved" as qualifier_is_approved_1,
    "Coded_Clinical_Entry_Sequence_Number" as coded_clinical_entry_sequence_number,
    "Coded_Clinical_Entry_Sequence_Number" as coded_clinical_entry_sequence_number_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterDiagnosisSNOMED') }}
