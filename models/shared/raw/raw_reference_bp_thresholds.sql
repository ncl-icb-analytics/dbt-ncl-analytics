-- Raw layer model for reference_analyst_managed.BP_THRESHOLDS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "THRESHOLD_RULE_ID" as threshold_rule_id,
    "PROGRAMME_OR_GUIDELINE" as programme_or_guideline,
    "DESCRIPTION" as description,
    "PATIENT_GROUP" as patient_group,
    "THRESHOLD_TYPE" as threshold_type,
    "SYSTOLIC_THRESHOLD" as systolic_threshold,
    "DIASTOLIC_THRESHOLD" as diastolic_threshold,
    "OPERATOR" as operator,
    "NOTES" as notes
from {{ source('reference_analyst_managed', 'BP_THRESHOLDS') }}
