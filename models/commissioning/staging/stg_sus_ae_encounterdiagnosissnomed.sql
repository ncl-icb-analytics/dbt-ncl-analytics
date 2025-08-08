-- Staging model for sus_ae.EncounterDiagnosisSNOMED
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Qualifier" as qualifier,
    "Qualifier_Is_Approved" as qualifier_is_approved,
    "Coded_Clinical_Entry_Sequence_Number" as coded_clinical_entry_sequence_number,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterDiagnosisSNOMED') }}
