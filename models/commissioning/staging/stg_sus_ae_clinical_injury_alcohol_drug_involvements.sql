-- Staging model for sus_ae.clinical.injury.alcohol_drug_involvements
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ALCOHOL_DRUG_INVOLVEMENTS_ID" as alcohol_drug_involvements_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_ae', 'clinical.injury.alcohol_drug_involvements') }}
