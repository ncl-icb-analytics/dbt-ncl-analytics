{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.ActiveSubmission \ndbt: source(''mhsds'', ''ActiveSubmission'') \nColumns:\n  OrgIDProvider -> org_id_provider\n  UniqSubmissionID -> uniq_submission_id\n  ReportingPeriodEndDate -> reporting_period_end_date"
    )
}}
select
    "OrgIDProvider" as org_id_provider,
    "UniqSubmissionID" as uniq_submission_id,
    "ReportingPeriodEndDate" as reporting_period_end_date
from {{ source('mhsds', 'ActiveSubmission') }}
