-- Staging model for dictionary_snomed.Primary_Care_Domain_Ref_Set
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "Cluster_ID" as cluster_id,
    "Cluster_Description" as cluster_description,
    "SNOMED_Code" as snomed_code,
    "SNOMED_Code_Description" as snomed_code_description,
    "PCD_Refset_ID" as pcd_refset_id,
    "Service_And_Ruleset" as service_and_ruleset,
    "IsActive" as isactive,
    "ActiveFrom" as activefrom,
    "ActiveTo" as activeto,
    "FirstCreated" as firstcreated,
    "LastUpdated" as lastupdated
from {{ source('dictionary_snomed', 'Primary_Care_Domain_Ref_Set') }}
