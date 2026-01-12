{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.participants \ndbt: source(''sus_ae'', ''clinical.participants'') \nColumns:\n  PARTICIPANTS_ID -> participants_id\n  identifier -> identifier\n  dmicImportLogId -> dmic_import_log_id\n  clinical_responsibility_timestamp -> clinical_responsibility_timestamp\n  issuer -> issuer\n  tier -> tier\n  has_discharge_responsibility -> has_discharge_responsibility\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id"
    )
}}
select
    "PARTICIPANTS_ID" as participants_id,
    "identifier" as identifier,
    "dmicImportLogId" as dmic_import_log_id,
    "clinical_responsibility_timestamp" as clinical_responsibility_timestamp,
    "issuer" as issuer,
    "tier" as tier,
    "has_discharge_responsibility" as has_discharge_responsibility,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_ae', 'clinical.participants') }}
