{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.NHSE_SERVICE_CODES_2526 \ndbt: source(''reference_lookup_ncl'', ''NHSE_SERVICE_CODES_2526'') \nColumns:\n  SERVICE_CODE -> service_code\n  SERVICE_CODE_DESCRIPTION -> service_code_description\n  SERVICE_CODE_INCLUDING_DESCRIPTION -> service_code_including_description\n  SERVICE_CATEGORY_DESCRIPTION -> service_category_description\n  National_Programme_of_Care_(NPoC)_Category_and_Clinical_Reference_Group_(CRG) -> national_programme_of_care_npo_c_category_and_clinical_reference_group_crg\n  HIGHLY_SPECIALISED_SERVICE -> highly_specialised_service\n  Identified_by_PS_Operational_Tool_2024/25 -> identified_by_ps_operational_tool_2024_25\n  NOTES -> notes\n  EFFECTIVE_FROM -> effective_from\n  EFFECTIVE_TO -> effective_to\n  IS_CURRENT -> is_current\n  2024/25_ICB_Delegation_Status -> status_2024_25_icb_delegation\n  2025/26_ICB_Delegation_Status -> status_2025_26_icb_delegation\n  DELEGATION_STATUS_CHANGE -> delegation_status_change\n  GROUPING -> grouping\n  AMENDED_RECORD_INDICATOR -> amended_record_indicator"
    )
}}
select
    "SERVICE_CODE" as service_code,
    "SERVICE_CODE_DESCRIPTION" as service_code_description,
    "SERVICE_CODE_INCLUDING_DESCRIPTION" as service_code_including_description,
    "SERVICE_CATEGORY_DESCRIPTION" as service_category_description,
    "National_Programme_of_Care_(NPoC)_Category_and_Clinical_Reference_Group_(CRG)" as national_programme_of_care_npo_c_category_and_clinical_reference_group_crg,
    "HIGHLY_SPECIALISED_SERVICE" as highly_specialised_service,
    "Identified_by_PS_Operational_Tool_2024/25" as identified_by_ps_operational_tool_2024_25,
    "NOTES" as notes,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_CURRENT" as is_current,
    "2024/25_ICB_Delegation_Status" as status_2024_25_icb_delegation,
    "2025/26_ICB_Delegation_Status" as status_2025_26_icb_delegation,
    "DELEGATION_STATUS_CHANGE" as delegation_status_change,
    "GROUPING" as grouping,
    "AMENDED_RECORD_INDICATOR" as amended_record_indicator
from {{ source('reference_lookup_ncl', 'NHSE_SERVICE_CODES_2526') }}
