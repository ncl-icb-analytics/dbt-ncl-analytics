{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.ONLINE_CONSULTATION \ndbt: source(''reference_analyst_managed'', ''ONLINE_CONSULTATION'') \nColumns:\n  MONTH -> month\n  GP_CODE -> gp_code\n  GP_NAME -> gp_name\n  PCN_NAME -> pcn_name\n  SUBMISSIONS -> submissions\n  CLINICAL_SUBMISSIONS -> clinical_submissions\n  ADMINISTRATIVE_SUBMISSIONS -> administrative_submissions\n  Other/Unknown Type Submissions -> other_unknown_type_submissions\n  REGISTERED_PATIENT_COUNT -> registered_patient_count\n  Rate per 1,000 Registered Patients -> rate_per_1,000_registered_patients"
    )
}}
select
    "MONTH" as month,
    "GP_CODE" as gp_code,
    "GP_NAME" as gp_name,
    "PCN_NAME" as pcn_name,
    "SUBMISSIONS" as submissions,
    "CLINICAL_SUBMISSIONS" as clinical_submissions,
    "ADMINISTRATIVE_SUBMISSIONS" as administrative_submissions,
    "Other/Unknown Type Submissions" as other_unknown_type_submissions,
    "REGISTERED_PATIENT_COUNT" as registered_patient_count,
    "Rate per 1,000 Registered Patients" as rate_per_1,000_registered_patients
from {{ source('reference_analyst_managed', 'ONLINE_CONSULTATION') }}
