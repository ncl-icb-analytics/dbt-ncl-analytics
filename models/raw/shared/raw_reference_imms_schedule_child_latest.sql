-- Raw layer model for reference_analyst_managed.IMMS_SCHEDULE_CHILD_LATEST
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "VACCINE_ORDER" as vaccine_order,
    "VACCINE_ID" as vaccine_id,
    "VACCINE_NAME" as vaccine_name,
    "DOSE_NUMBER" as dose_number,
    "DISEASES_PROTECTED_AGAINST" as diseases_protected_against,
    "VACCINE_CODE" as vaccine_code,
    "TRADE_NAME" as trade_name,
    "SCHEDULE_AGE" as schedule_age,
    "ELIGIBLE_AGE_FROM_DAYS" as eligible_age_from_days,
    "ELIGIBLE_AGE_TO_DAYS" as eligible_age_to_days,
    "ADMINISTERED_CLUSTER_ID" as administered_cluster_id,
    "DRUG_CLUSTER_ID" as drug_cluster_id,
    "DECLINED_CLUSTER_ID" as declined_cluster_id,
    "CONTRAINDICATED_CLUSTER_ID" as contraindicated_cluster_id
from {{ source('reference_analyst_managed', 'IMMS_SCHEDULE_CHILD_LATEST') }}
