{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.PCD_REFSET_LATEST \ndbt: source(''reference_terminology'', ''PCD_REFSET_LATEST'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_CODE_DESCRIPTION -> snomed_code_description\n  PCD_REFSET_ID -> pcd_refset_id\n  SERVICE_AND_RULESET -> service_and_ruleset"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_CODE_DESCRIPTION" as snomed_code_description,
    "PCD_REFSET_ID" as pcd_refset_id,
    "SERVICE_AND_RULESET" as service_and_ruleset
from {{ source('reference_terminology', 'PCD_REFSET_LATEST') }}
