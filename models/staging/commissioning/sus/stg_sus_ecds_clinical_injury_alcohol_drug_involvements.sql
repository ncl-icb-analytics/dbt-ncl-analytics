-- Staging layer: Alcohol and drug involvement codes recorded against an injury-related ECDS attendance. Multiple rows per attendance.
-- 1:1 with raw_sus_ecds_clinical_injury_alcohol_drug_involvements - column renaming only, no joins or filtering.

select
    rownumber_id,
    primarykey_id,
    alcohol_drug_involvements_id,
    code,
    is_code_approved,
    dmic_import_log_id
from {{ ref('raw_sus_ecds_clinical_injury_alcohol_drug_involvements') }}
