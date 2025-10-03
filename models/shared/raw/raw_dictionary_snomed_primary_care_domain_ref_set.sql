-- Raw layer model for dictionary_snomed.Primary_Care_Domain_Ref_Set
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed
-- This is a 1:1 passthrough from source with standardized column names
select
    "Cluster_ID" as cluster_id,
    "Cluster_Description" as cluster_description,
    "SNOMED_Code" as snomed_code,
    "SNOMED_Code_Description" as snomed_code_description,
    "PCD_Refset_ID" as pcd_refset_id,
    "Service_And_Ruleset" as service_and_ruleset,
    "IsActive" as is_active,
    "ActiveFrom" as active_from,
    "ActiveTo" as active_to,
    "FirstCreated" as first_created,
    "LastUpdated" as last_updated
from {{ source('dictionary_snomed', 'Primary_Care_Domain_Ref_Set') }}
