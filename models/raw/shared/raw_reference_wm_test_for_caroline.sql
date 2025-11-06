-- Raw layer model for reference_analyst_managed.WM_TEST_FOR_CAROLINE
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "TRUST" as trust,
    "TRUST_PATIENT_ID" as trust_patient_id,
    "HOSPITALNUMBER" as hospitalnumber,
    "INTERVENTION" as intervention,
    "DIAGNOSIS" as diagnosis,
    "PREVIOUS_DRUG_1" as previous_drug_1,
    "PREVIOUS_DRUG_2" as previous_drug_2,
    "PREVIOUS_DRUG_3" as previous_drug_3,
    "PREVIOUS_DRUG_4" as previous_drug_4,
    "PREVIOUS_DRUG_5" as previous_drug_5,
    "PREVIOUS_DRUG_6" as previous_drug_6
from {{ source('reference_analyst_managed', 'WM_TEST_FOR_CAROLINE') }}
