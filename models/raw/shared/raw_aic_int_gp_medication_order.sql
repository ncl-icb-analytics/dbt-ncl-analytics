-- Raw layer model for aic.INT_GP_MEDICATION_ORDER
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEDICATION_ORDER_ID" as medication_order_id,
    "MEDICATION_STATEMENT_ID" as medication_statement_id,
    "PERSON_ID" as person_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "MEDICATION_CONCEPT_CODE" as medication_concept_code,
    "MEDICATION_CONCEPT_NAME" as medication_concept_name,
    "VMP_CONCEPT_CODE" as vmp_concept_code,
    "VMP_CONCEPT_NAME" as vmp_concept_name,
    "VTM_CONCEPT_CODE" as vtm_concept_code,
    "VTM_CONCEPT_NAME" as vtm_concept_name,
    "BNF_REFERENCE" as bnf_reference,
    "DOSE_INSTRUCTIONS" as dose_instructions,
    "QUANTITY_VALUE" as quantity_value,
    "QUANTITY_UNIT" as quantity_unit,
    "DURATION_DAYS" as duration_days,
    "ESTIMATED_COST" as estimated_cost,
    "AGE_AT_EVENT" as age_at_event
from {{ source('aic', 'INT_GP_MEDICATION_ORDER') }}
