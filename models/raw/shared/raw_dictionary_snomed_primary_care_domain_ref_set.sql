{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Primary_Care_Domain_Ref_Set \ndbt: source(''dictionary_snomed'', ''Primary_Care_Domain_Ref_Set'') \nColumns:\n  Cluster_ID -> cluster_id\n  Cluster_Description -> cluster_description\n  SNOMED_Code -> snomed_code\n  SNOMED_Code_Description -> snomed_code_description\n  PCD_Refset_ID -> pcd_refset_id\n  Service_And_Ruleset -> service_and_ruleset\n  IsActive -> is_active\n  ActiveFrom -> active_from\n  ActiveTo -> active_to\n  FirstCreated -> first_created\n  LastUpdated -> last_updated"
    )
}}
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
