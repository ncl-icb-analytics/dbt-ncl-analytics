-- Raw layer model for reference_analyst_managed.FA__MONTHLY_NHSE_SERVICE_CODES_2526
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
from {{ source('reference_analyst_managed', 'FA__MONTHLY_NHSE_SERVICE_CODES_2526') }}
